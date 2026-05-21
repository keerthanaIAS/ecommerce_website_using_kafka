const kafka = require("../shared/kafka/client");
const { send } = require("../shared/kafka/producer");
const redis = require("../shared/redis/client");

const consumer = kafka.consumer({
    groupId: "payment-group",
});

let paused = false;
let overloaded = false;

setInterval(() => {
    overloaded = Math.random() > 0.7;
    console.log("SYSTEM OVERLOAD:", overloaded);
}, 10000);

async function start() {
    await consumer.connect();
    await consumer.subscribe({
        topic: "inventory-reserved",
        fromBeginning: true
    });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({
            topic,
            partition,
            message
        }) => {
            console.log({
                topic,
                partition,
                offset: message.offset,
                paused
            });

            if (overloaded && !paused) {
                paused = true;
                console.log("PAUSING PAYMENT CONSUMER");
                consumer.pause([{ topic }]);
                setTimeout(async () => {
                    console.log("RESUMING PAYMENT CONSUMER");
                    consumer.resume([{ topic }]);
                    // Now you explicitly told Kafka:
                    // “Reset your internal fetch pointer back to offset 7.”

                    // That changes Kafka’s state, not your JS state.

                    // Then Kafka must do:
                    // fetch offset 7 again
                    // call eachMessage again.
                    await consumer.seek({
                        topic,
                        partition,
                        offset: message.offset
                    });
                    paused = false;
                    overloaded = false;
                }, 5000);
                // message will be reprocessed after resume
                return;
            }
            // after put seek it will call again eachmessages right? in that state using radmon it overflow may be false so it called shipping

            const order = JSON.parse(
                message.value.toString()
            );

            const processed = await redis.get(
                `processed:${order.orderId}`
            );

            if (processed) {
                console.log(
                    `Order ${order.orderId} already processed`
                );
                // only commit when already processed
                const nextOffset = (BigInt(message.offset) + 1n).toString();
                await consumer.commitOffsets([
                    {
                        topic,
                        partition,
                        offset: nextOffset
                    }
                ]);
                return;
            }

            await new Promise(resolve =>
                setTimeout(resolve, 2000)
            );

            await redis.set(
                `processed:${order.orderId}`,
                "1"
            );

            await send(
                "payment-success",
                order,
                order.orderId
            );

            const nextOffset =
                (
                    BigInt(message.offset) + 1n
                ).toString();

            // only commit after business work is done
            await consumer.commitOffsets([
                {
                    topic,
                    partition,
                    offset: nextOffset
                }
            ]);
        }
    });

    return consumer;
}

module.exports = start;
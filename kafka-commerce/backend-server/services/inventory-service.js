const kafka = require("../shared/kafka/client");
const send = require("../shared/kafka/producer");

const consumer = kafka.consumer({
    groupId: "inventory-group",
});

async function start() {
    await consumer.connect();

    await consumer.subscribe({ topic: "order-created" });

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({ message, heartbeat, commitOffsetsIfNecessary }) => {
            const order = JSON.parse(message.value.toString());
            console.log("processing:", order.orderId);
            await new Promise(r => setTimeout(r, 2000));
            await commitOffsetsIfNecessary();
        }
    });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const order = JSON.parse(message.value.toString());

            const retries = parseInt(message.headers?.retry || "0");

            console.log("inventory processing:", order.orderId);

            const success = Math.random() > 0.3;

            if (success) {
                await send("inventory-reserved", order, order.orderId);
            } else {
                if (retries < 3) {
                    await send(
                        "inventory-retry",
                        order,
                        order.orderId,
                        {
                            retry: Buffer.from(String(retries + 1)),
                        }
                    );
                } else {
                    await send("dead-letter-orders", order, order.orderId);
                }
            }
        },
    });
}

module.exports = start;
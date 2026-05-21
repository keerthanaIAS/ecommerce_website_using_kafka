const kafka = require("../shared/kafka/client");
const { connectMongo } = require("../shared/db/mongo");
const redis = require("../shared/redis/client");
const { send } = require("../shared/kafka/producer");

const consumer = kafka.consumer({
    groupId: "shipping-group",
});

async function start() {
    await consumer.connect();
    await consumer.subscribe({ topic: "payment-success", fromBeginning: true });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {

            console.log({
                topic,
                partition,
                offset: message.offset,
                key: message.key?.toString(),
            });

            let order;
            try {
                order = JSON.parse(message.value.toString());
                const db = await connectMongo();

                const already = await redis.get(`shipped:${order.orderId}`);
                if (already) return;

                await db.collection("orders").updateOne(
                    { orderId: order.orderId },
                    { $set: { status: "SHIPPED" } }
                );

                await redis.set(`shipped:${order.orderId}`, "1");
                await send("shipment-created", order, order.orderId);

                console.log("shipped:", order.orderId);
            } catch (err) {
                console.error("shipping error:", err);
                await send("dead-letter-orders", {
                    error: err.message,
                    payload: order || message.value.toString()
                });
            }

            // Manual commit checkpoint execution block
            const nextOffset = (BigInt(message.offset) + 1n).toString();
            await consumer.commitOffsets([{ topic, partition, offset: nextOffset }]);
        },
    });

    return consumer;
}

module.exports = start;
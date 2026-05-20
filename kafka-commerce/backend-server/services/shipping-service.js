const kafka = require("../shared/kafka/client");
const connectMongo = require("../shared/db/mongo");
const redis = require("../shared/redis/client");
const send = require("../shared/kafka/producer");

const consumer = kafka.consumer({
    groupId: "shipping-group",
});

async function start() {
    await consumer.connect();
    await consumer.subscribe({ topic: "payment-success" });

    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const order = JSON.parse(message.value.toString());

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
                await send("dead-letter-orders", { error: err.message });
            }
        },
    });
}

module.exports = start;
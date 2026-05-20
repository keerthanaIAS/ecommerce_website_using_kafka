const kafka = require("../shared/kafka/client");
const connectMongo = require("../shared/db/mongo");
const redis = require("../shared/redis/client");

const consumer = kafka.consumer({
    groupId: "shipping-group"
});

async function start() {
    await consumer.connect();

    await consumer.subscribe({
        topic: "payment-success"
    });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const db = await connectMongo();

            const order = JSON.parse(message.value);

            await db.collection("orders").updateOne(
                { orderId: order.orderId },
                { $set: { status: "SHIPPED" } }
            );

            await redis.set(order.orderId, "SHIPPED");

            console.log("shipping created", order);
        }
    });
}

module.exports = start;
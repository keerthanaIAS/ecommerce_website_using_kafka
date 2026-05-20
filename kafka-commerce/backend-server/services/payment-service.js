const kafka = require("../shared/kafka/client");
const send = require("../shared/kafka/producer");
const redis = require("../shared/redis/client");

const consumer = kafka.consumer({
    groupId: "payment-group",
});

async function start() {
    await consumer.connect();
    await consumer.subscribe({ topic: "inventory-reserved" });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const order = JSON.parse(message.value.toString());

            // Idempotency
            const processed = await redis.get(`processed:${order.orderId}`);
            if (processed) return;

            const success = Math.random() > 0.2;

            if (success) {
                await redis.set(`processed:${order.orderId}`, "1");
                await send("payment-success", order, order.orderId);
            } else {
                await send("payment-failed", order, order.orderId);
            }
        },
    });
}

module.exports = start;
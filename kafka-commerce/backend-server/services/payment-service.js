const kafka = require("../shared/kafka/client");
const { send } = require("../shared/kafka/producer");
const redis = require("../shared/redis/client");

const consumer = kafka.consumer({
    groupId: "payment-group",
});

async function start() {
    await consumer.connect();
    await consumer.subscribe({ topic: "inventory-reserved", fromBeginning: true });

    await consumer.run({
        autoCommit: false, // Turned off to prevent early automatic tracking
        eachMessage: async ({ topic, partition, message }) => {
            const order = JSON.parse(message.value.toString());

            const processed = await redis.get(`processed:${order.orderId}`);
            if (processed) {
                console.log(`Order ${order.orderId} already processed by payments. Skipping.`);
                return;
            }

            const success = Math.random() > 0.2;

            if (success) {
                await redis.set(`processed:${order.orderId}`, "1");
                await send("payment-success", order, order.orderId);
            } else {
                await send("payment-failed", order, order.orderId);
            }

            // Manual commit after routing logic satisfies completely
            const nextOffset = (BigInt(message.offset) + 1n).toString();
            await consumer.commitOffsets([{ topic, partition, offset: nextOffset }]);
        },
    });

    return consumer;
}

module.exports = start;
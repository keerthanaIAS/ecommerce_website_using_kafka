const kafka = require("../shared/kafka/client");
const { send } = require("../shared/kafka/producer");

const consumer = kafka.consumer({
    groupId: "inventory-group",
});

async function start() {
    await consumer.connect();
    // Subscribing to both operational topics cleanly
    await consumer.subscribe({ topics: ["order-created", "inventory-retry"], fromBeginning: true });

    await consumer.run({
        autoCommit: false, // Ensures accurate transaction control checkpoints
        eachMessage: async ({ topic, partition, message, heartbeat }) => {
            const order = JSON.parse(message.value.toString());
            const retries = parseInt(message.headers?.retry?.toString() || "0");

            console.log(`Processing ${topic} -> Order ID: ${order.orderId} (Attempt: ${retries})`);

            try {
                // Heartbeat to keep processing allocation healthy during task windows
                await heartbeat();

                // Randomized simulation engine window
                const success = Math.random() > 0.3;

                if (success) {
                    await send("inventory-reserved", order, order.orderId);
                } else {
                    if (retries < 3) {
                        await send(
                            "inventory-retry",
                            order,
                            order.orderId,
                            { retry: Buffer.from(String(retries + 1)) }
                        );
                    } else {
                        // Preserving explicit payload structures safely inside the DLQ
                        await send("dead-letter-orders", {
                            error: "Inventory allocation failed after max structural retries",
                            topic,
                            payload: order
                        }, order.orderId);
                    }
                }

                // Explicitly commit progress manually ONLY after success downstream routing paths confirm
                const nextOffset = (BigInt(message.offset) + 1n).toString();
                await consumer.commitOffsets([
                    { topic, partition, offset: nextOffset }
                ]);

            } catch (err) {
                console.error(`Fatal processing error for order: ${order.orderId}`, err);
                // Intentionally omit offset updates here so it will fall back to retry windows
            }
        }
    });

    return consumer;
}

module.exports = start;
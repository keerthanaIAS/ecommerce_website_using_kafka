const kafka = require("../shared/kafka/client");
const send = require("../shared/kafka/producer");

const consumer = kafka.consumer({
    groupId: "payment-group"
});

async function start() {
    await consumer.connect();

    await consumer.subscribe({
        topic: "inventory-reserved"
    });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const order = JSON.parse(message.value);

            const success = Math.random() > 0.2;

            if (success) {
                await send("payment-success", order);
            } else {
                await send("payment-failed", order);
            }
        }
    });
}

module.exports = start;
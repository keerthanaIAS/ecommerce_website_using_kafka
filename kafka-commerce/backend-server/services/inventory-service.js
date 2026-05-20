const kafka = require("../shared/kafka/client");
const send = require("../shared/kafka/producer");

const consumer = kafka.consumer({
    groupId: "inventory-group"
});

async function start() {
    await consumer.connect();

    await consumer.subscribe({
        topic: "order-created"
    });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const order = JSON.parse(message.value);

            console.log("inventory checking", order);

            const success = Math.random() > 0.2;

            if (success) {
                await send("inventory-reserved", order);
            } else {
                await send("inventory-failed", order);
            }
        }
    });
}

module.exports = start;
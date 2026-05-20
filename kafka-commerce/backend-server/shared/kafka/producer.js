const kafka = require("./client");

const producer = kafka.producer({
    idempotent: true, // Ensures exactly-once delivery semantics
    maxInFlightRequests: 5
});

let isConnected = false;

async function initProducer() {
    if (!isConnected) {
        await producer.connect();
        isConnected = true;
    }
}

async function send(topic, message, key = null, headers = {}) {
    await initProducer();

    await producer.send({
        topic,
        messages: [
            {
                key: key ? String(key) : null,
                value: JSON.stringify(message),
                headers,
            },
        ],
    });

    console.log(`sent → ${topic}`);
}

// Clean connection termination hook
async function disconnectProducer() {
    if (isConnected) {
        await producer.disconnect();
        isConnected = false;
        console.log("Kafka producer disconnected cleanly.");
    }
}

module.exports = { send, disconnectProducer };
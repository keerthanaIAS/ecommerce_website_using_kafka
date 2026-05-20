const kafka = require("./client");

const producer = kafka.producer();
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
                key, // partition control
                value: JSON.stringify(message),
                headers,
            },
        ],
    });

    console.log(`sent → ${topic}`);
}

module.exports = send;
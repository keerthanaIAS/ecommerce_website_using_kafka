const kafka = require("./client");

const producer = kafka.producer();

async function send(topic, message) {
    await producer.connect();

    await producer.send({
        topic,
        messages: [
            {
                value: JSON.stringify(message)
            }
        ]
    });

    console.log(`sent to ${topic}`);
}

module.exports = send;
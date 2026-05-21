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

function getRetryPartition(retryCount) {
    if (retryCount === 1) return 0;
    if (retryCount === 2) return 1;
    return 2;
}

async function send(topic, message, key = null, headers = {}) {
    await initProducer();

    const retry =
        parseInt(headers.retry?.toString() || "0");

    const kafkaMessage = {
        key: key ? String(key) : null,
        value: JSON.stringify(message),
        headers
    };

    if (topic === "inventory-retry") {
        kafkaMessage.partition =
            getRetryPartition(retry);
    }

    const result = await producer.send({
        topic,
        messages: [kafkaMessage]
    });

    const partition = result[0].partition;

    console.log("KAFKA SEND METADATA:", JSON.stringify(result, null, 2));

    const admin = kafka.admin();
    await admin.connect();

    const meta = await admin.fetchTopicMetadata({ topics: [topic] });

    console.log(JSON.stringify(meta, null, 2));

    const leader = meta.topics[0].partitions.find(
        p => p.partitionId === partition
    )?.leader;

    console.log({
        topic,
        partition,
        leaderBroker: leader,
        offset: result[0].baseOffset
    });

    await admin.disconnect();

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
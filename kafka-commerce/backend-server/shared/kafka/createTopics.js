const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "admin-client",
    brokers: ["localhost:9092"]
});

const topics = [
    { topic: "order-created", numPartitions: 4 },
    { topic: "inventory-reserved", numPartitions: 2 },
    { topic: "inventory-failed", numPartitions: 2 },
    { topic: "payment-success", numPartitions: 2 },
    { topic: "payment-failed", numPartitions: 2 },
    { topic: "shipment-created", numPartitions: 2 },
    { topic: "dead-letter-orders", numPartitions: 2 }
];

async function createTopics() {
    const admin = kafka.admin();

    await admin.connect();

    await admin.createTopics({
        topics
    });

    console.log("Topics created");

    await admin.disconnect();
}

createTopics().catch(console.error);
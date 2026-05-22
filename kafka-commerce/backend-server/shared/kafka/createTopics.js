const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "admin-client",
    brokers: [
        "192.168.1.21:9092", // ip wise check
    ],
    ssl: false,
    sasl: {                  // sasl username and password
        mechanism: "plain",
        username: "keerthana",
        password: "Tara@123"
    },
    retry: {
        initialRetryTime: 100,
        retries: 8
    }
});

const topics = [
    { topic: "order-created", numPartitions: 4 },
    { topic: "inventory-retry", numPartitions: 2 },
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
        topics,
        waitForLeaders: true // Forces the code to wait until partitions are fully hosted
    });
    console.log("Topics synchronized on broker.");
    console.log("Topics created successfully");
    await admin.disconnect();
}

createTopics().catch(console.error);

module.exports = { createTopics };
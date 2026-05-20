const { Kafka } = require("kafkajs");

module.exports = new Kafka({
    clientId: "commerce-app",
    brokers: ["localhost:9092"],
    retry: {
        initialRetryTime: 100,
        retries: 8
    }
});
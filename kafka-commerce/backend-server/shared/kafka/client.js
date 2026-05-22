const { Kafka } = require("kafkajs");

module.exports = new Kafka({
    clientId: "commerce-app",
    brokers: [
        "192.168.1.21:9092",     // ip wise check - localhost to ip
    ],
    ssl: false,
    sasl: {
        mechanism: "plain",
        username: "keerthana",
        password: "Tara@123"
    },
    retry: {
        initialRetryTime: 100,
        retries: 8
    }
});
// Replication factor = number of copies of each partition across brokers, 
// so if one broker dies, data is still available from another broker without losing messages or breaking your pipeline. 
// With 3 brokers, Kafka can keep 1 leader + 2 replicas, 
// giving you fault tolerance + high availability + safe consumer continuity during failures or rebalancing.
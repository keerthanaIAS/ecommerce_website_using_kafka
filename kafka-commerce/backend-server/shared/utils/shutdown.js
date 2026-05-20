const redis = require("../shared/redis/client");
const { closeMongo } = require("../shared/db/mongo");
const { disconnectProducer } = require("../shared/kafka/producer");

const resources = {
    server: null,
    consumers: []
};

function registerServer(serverInstance) {
    resources.server = serverInstance;
}

function registerConsumer(consumerInstance, serviceName) {
    resources.consumers.push({ instance: consumerInstance, name: serviceName });
}

async function handleGracefulShutdown(signal) {
    console.log(`\n[${signal}] Received. Initiating graceful shutdown orchestration...`);

    // 1. Await Express closure completely before breaking data tiers
    if (resources.server) {
        await new Promise((resolve) => {
            resources.server.close(() => {
                console.log("Express HTTP server ceased accepting connections cleanly.");
                resolve();
            });
        });
    }

    // 2. Disconnect all Kafka consumers cleanly to trigger instant broker rebalances
    for (const consumer of resources.consumers) {
        try {
            await consumer.instance.disconnect();
            console.log(`Kafka Consumer [${consumer.name}] disconnected cleanly.`);
        } catch (err) {
            console.error(`Error disconnecting Kafka Consumer [${consumer.name}]:`, err);
        }
    }

    // 3. Disconnect background infrastructure resources safely
    try {
        await disconnectProducer();
        await closeMongo();
        
        await redis.quit();
        console.log("Redis connection closed cleanly.");

        console.log("All components shut down safely. Exiting process.");
        process.exit(0);
    } catch (err) {
        console.error("Fatal error encountered during graceful cleanup loop:", err);
        process.exit(1);
    }
}

process.on("SIGTERM", () => handleGracefulShutdown("SIGTERM"));
process.on("SIGINT", () => handleGracefulShutdown("SIGINT"));

module.exports = {
    registerServer,
    registerConsumer
};
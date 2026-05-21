// index.js
const express = require("express");
const cors = require("cors");

// Routes & Resource Utilities
const orderRoutes = require("./services/order-service");
const { registerServer, registerConsumer } = require("./shared/utils/shutdown");

// Bring in your Topic Creation Function and Services
const { createTopics } = require("./shared/kafka/createTopics");
const startInventory = require("./services/inventory-service");
const startPayment = require("./services/payment-service");
const startShipping = require("./services/shipping-service");

const app = express();
app.use(cors());
app.use(express.json());
app.use(orderRoutes);

const server = app.listen(3001, () => {
    console.log("Monolithic Core Server running on port 3001");
});
registerServer(server);

// Securely orchestrate the startup sequence
async function bootstrapWorkers() {
    try {
        console.log("Step 1: Synchronizing Kafka cluster topics...");
        // This blocks consumers from starting until topics are verified/created on the broker
        await createTopics(); 

        console.log("Step 2: Initializing microservice consumption workers...");

        const inventoryConsumer = await startInventory();
        registerConsumer(inventoryConsumer, "Inventory Service");

        const paymentConsumer = await startPayment();
        registerConsumer(paymentConsumer, "Payment Service");

        const shippingConsumer = await startShipping();
        registerConsumer(shippingConsumer, "Shipping Service");

        console.log("All background pipeline microservices activated safely.");
    } catch (err) {
        console.error("Core initialization sequence crashed:", err);
        process.exit(1);
    }
}

bootstrapWorkers();
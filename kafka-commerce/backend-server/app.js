const express = require("express");
const cors = require("cors");

// Routes
const orderRoutes = require("./routes/orderRoutes");

// Services
const startInventory = require("./services/inventoryService");
const startPayment = require("./services/paymentService");
const startShipping = require("./services/shippingService");

// Reusable Shutdown Manager Utility
const { registerServer, registerConsumer } = require("./utils/shutdown");

const app = express();

app.use(cors());
app.use(express.json());

// Inject routes
app.use(orderRoutes);

// 1. Start HTTP Server and capture running instance
const server = app.listen(3001, () => {
    console.log("Monolithic Core Server running on port 3001");
});

// Register server reference for clean shutdown
registerServer(server);

// 2. Initialize and register long-running background asynchronous Kafka worker engines
async function bootstrapWorkers() {
    try {
        console.log("Initializing microservice consumption workers...");

        const inventoryConsumer = await startInventory();
        registerConsumer(inventoryConsumer, "Inventory Service");

        const paymentConsumer = await startPayment();
        registerConsumer(paymentConsumer, "Payment Service");

        const shippingConsumer = await startShipping();
        registerConsumer(shippingConsumer, "Shipping Service");

        console.log("All background pipeline microservices activated.");
    } catch (err) {
        console.error("Core initialization sequence crashed:", err);
        process.exit(1);
    }
}

bootstrapWorkers();
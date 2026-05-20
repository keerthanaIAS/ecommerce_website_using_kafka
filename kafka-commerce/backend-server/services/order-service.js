const express = require("express");
const { v4: uuid } = require("uuid");
const { send } = require("../shared/kafka/producer");
const { connectMongo } = require("../shared/db/mongo");
const redis = require("../shared/redis/client");

const router = express.Router();

router.post("/order", async (req, res) => {
    try {
        const db = await connectMongo();

        const order = {
            orderId: uuid(),
            product: req.body.product,
            status: "CREATED",
        };

        // Write to Primary DB
        await db.collection("orders").insertOne(order);

        // Seed initial state in Redis cache
        await redis.set(`order:${order.orderId}`, "CREATED");

        // Discard into processing ecosystem streams
        await send(
            "order-created",
            order,
            order.orderId
        );

        res.json(order);
    } catch (err) {
        console.error("Order Creation Error:", err);
        res.status(500).json({ error: "Failed to place order" });
    }
});

router.get("/orders", async (req, res) => {
    const db = await connectMongo();
    const orders = await db.collection("orders").find({}).sort({ _id: -1 }).toArray();
    res.json(orders);
});

router.get("/order/:id", async (req, res) => {
    const db = await connectMongo();
    const order = await db.collection("orders").findOne({ orderId: req.params.id });
    res.json(order);
});

module.exports = router;
const express = require("express");
const { v4: uuid } = require("uuid");

const send = require("../shared/kafka/producer");
const connectMongo = require("../shared/db/mongo");
const redis = require("../shared/redis/client");

const router = express.Router();

router.post("/order", async (req, res) => {
    const db = await connectMongo();

    const order = {
        orderId: uuid(),
        product: req.body.product,
        status: "CREATED"
    };

    await db.collection("orders").insertOne(order);

    const value = await redis.set(order.orderId, "CREATED");
    console.log("redis verify:", value);

    await send("order-created", order);

    res.json(order);
});

module.exports = router;
const express = require("express");
const cors = require("cors");

const orderRoutes = require("./services/order-service");

const inventory = require("./services/inventory-service");
const payment = require("./services/payment-service");
const shipping = require("./services/shipping-service");

const app = express();

app.use(cors());
app.use(express.json());

app.use(orderRoutes);

app.listen(3001, () => {
    console.log("server running");
});

inventory();
payment();
shipping();
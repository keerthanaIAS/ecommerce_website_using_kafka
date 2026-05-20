const redis = require("redis");

const client = redis.createClient({
    url: "redis://localhost:6379"
});

client.connect();

client.on("connect", () => {
    console.log("Redis connected");
});

module.exports = client;
const { MongoClient } = require("mongodb");

const url = "mongodb://localhost:27017";
const client = new MongoClient(url);
let db = null;

async function connectMongo() {
    if (!db) {
        await client.connect();
        db = client.db("commerce_db");
    }
    return db;
}

async function closeMongo() {
    await client.close();
    db = null;
    console.log("MongoDB connection closed cleanly.");
}

module.exports = { connectMongo, closeMongo };
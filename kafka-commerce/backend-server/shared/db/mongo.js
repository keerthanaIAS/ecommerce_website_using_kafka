const { MongoClient } = require("mongodb");

const client = new MongoClient("mongodb://localhost:27017");

async function connectMongo() {
    await client.connect();
    console.log("Mongo connected");
    return client.db("commerce");
}

module.exports = connectMongo;
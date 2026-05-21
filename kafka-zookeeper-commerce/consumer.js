const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "demo",
  brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({
  groupId: "order-group"
});

async function run() {
  await consumer.connect();
  await consumer.subscribe({
    topic: "orders",
    fromBeginning: true
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      console.log("received:", order);
    }
  });
}

run();
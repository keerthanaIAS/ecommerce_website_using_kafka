const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "demo",
  brokers: ["localhost:9092"]
});

const producer = kafka.producer();

async function run() {
  await producer.connect();

  setInterval(async () => {
    const msg = {
      orderId: Math.floor(Math.random() * 1000),
      status: "CREATED"
    };

    await producer.send({
      topic: "orders",
      messages: [
        {
          key: String(msg.orderId),
          value: JSON.stringify(msg)
        }
      ]
    });

    console.log("sent:", msg);
  }, 3000);
}

run();
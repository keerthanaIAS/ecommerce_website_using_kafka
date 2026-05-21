const kafka = require("./shared/kafka/client");

async function increasePartitions() {
    const admin = kafka.admin();

    await admin.connect();

    await admin.createPartitions({
        topicPartitions: [
            {
                topic: "order-created",
                count: 6
            }
        ]
    });

    console.log("Partitions increased");

    await admin.disconnect();
}

increasePartitions();

// Important Kafka rule:

// You can:
// increase partitions

// You cannot:
// decrease partitions
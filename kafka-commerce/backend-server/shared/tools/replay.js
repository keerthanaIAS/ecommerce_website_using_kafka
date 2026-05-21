const kafka = require("../kafka/client");

async function replay(topic, partition, offset) {
    const consumer = kafka.consumer({ groupId: "replay-tool" });

    await consumer.connect();

    await consumer.subscribe({ topic });

    await consumer.run({
        autoCommit: false,

        eachMessage: async ({
            topic,
            partition,
            message
        }) => {

            console.log("Replaying:", {
                topic,
                partition,
                offset: message.offset
            });

            const nextOffset =
                (BigInt(message.offset) + 1n).toString();

            await consumer.commitOffsets([
                {
                    topic,
                    partition,
                    offset: nextOffset
                }
            ]);
        }
    });

    await consumer.seek({
        topic,
        partition,
        offset: String(offset) // Kafka starts reading again --- You must manually provide the offset number.
    });
}

replay("dead-letter-orders", 0, "0");


// Replay call

// Right now:
// replay("dead-letter-orders", 0, "0");

// means:
// Replay from beginning of partition 0

// If later you want only from offset 8:
// replay("dead-letter-orders", 0, "8");
// Then Kafka skips 0–7 and starts from 8.
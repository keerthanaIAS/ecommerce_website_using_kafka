4 services:

1. Order Service (producer)
Creates order in MongoDB
Writes status to Redis
Publishes order-created
2. Inventory Consumer
Listens order-created
Randomly approves/rejects
Emits:
inventory-reserved OR
inventory-failed
3. Payment Consumer
Listens inventory-reserved
Randomly succeeds/fails
Emits:
payment-success OR
payment-failed
4. Shipping Consumer
Listens payment-success
Marks order SHIPPED in MongoDB + Redis
Emits shipment-created

Flow:

ORDER CREATED
   ↓
INVENTORY CHECK
   ↓
PAYMENT
   ↓
SHIPPING

END-TO-END FLOW (REAL INTERNAL VIEW):
Example order flow:
POST /order
  → MongoDB: order CREATED
  → Redis: CREATED
  → Kafka: order-created (offset 0, partition random)
Inventory consumer:
reads order-created
→ processes
→ writes inventory-reserved event
→ Kafka stores in partition log
→ offset committed in inventory-group
Payment consumer:
reads inventory-reserved
→ processes
→ emits payment-success
→ offset stored in payment-group
Shipping consumer:
reads payment-success
→ updates Mongo + Redis
→ emits shipment-created
→ offset stored in shipping-group



HOW TO INSPECT PARTITIONS + OFFSETS (YOU ASKED THIS)

Now real tools.

1. List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list
2. Describe topic (partitions)
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic order-created

You’ll see:

PartitionCount: 4
Partition: 0 Leader: 1 Replicas: 1 Isr: 1
Partition: 1 ...
3. See consumer group + offsets
VERY IMPORTANT COMMAND:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group inventory-group

You will see:

TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-created   0          10              15              5
order-created   1          20              20              0
Meaning:
CURRENT-OFFSET → last processed
LOG-END-OFFSET → latest message
LAG → backlog (CRITICAL metric)
4. See all consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
5. Reset offsets (danger tool)
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group inventory-group \
  --topic order-created \
  --reset-offsets --to-earliest --execute

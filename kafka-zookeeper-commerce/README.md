ZooKeeper → manages Kafka brokers
Kafka → stores messages
UI → displays Kafka state
Consumer → reads messages
Producer → writes messages

ZooKeeper is invisible to you because:

1 Kafka handles it internally
2 UI does not show ZooKeeper
3 Your app never talks to it directly

example:
Kafka system = factory
ZooKeeper = manager office (hidden)
Kafka = conveyor belt (real system)
Producer = machine adding items
Consumer = machine removing items
Kafka UI = CCTV monitor
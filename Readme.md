kafka-crash-course

Install confluent-kafka dependency
pip3 install confluent-kafka

Validate that the topic was created in kafka container
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

Describe that topic and see its partitions
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic new_orders

View all events in a topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning

## `consumer.poll(1.0)` (tracker.py)

- **What it does:** Asks the Kafka client for the next message from subscribed topics (here: `orders`, `notifications`). The `1.0` is a **timeout in seconds**—how long to wait if nothing is available yet.
- **Return value:**
  - **`None`** — No message arrived within 1 second (not an error). The loop continues and polls again.
  - **`Message`** — A record with payload, topic, partition, offset, etc. Your code then decodes `msg.value()` and commits the offset.
  - **Error on message** — `msg.error()` can be set (e.g. partition EOF, broker issues). Handle separately from `None`.
- **Why use a timeout:** Keeps the `while True` loop responsive (you can stop with Ctrl+C) instead of blocking forever on each call.
- **Behind the scenes:** While polling, the client maintains the group (`order-tracker`), fetches from brokers, and respects `auto.offset.reset: earliest` and manual `commit()` after each successful read.
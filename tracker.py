import json
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'order-tracker', 
    'auto.offset.reset': 'earliest', 
    'enable.auto.commit': False,
    'security.protocol': 'PLAINTEXT'
})

consumer.subscribe(['orders', 'notifications'])
print("🟢 Consumer is running and subscribed to orders topic")


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue
        print(f"🟢 Received message from topic {msg.topic()}: {msg.value().decode('utf-8')}")
        consumer.commit()
except KeyboardInterrupt:
    print("🔴 Consumer interrupted by user")
finally:
    consumer.close()

print("🟢 Consumer closed")
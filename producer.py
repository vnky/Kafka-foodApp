import json
import uuid
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})


def delivery_report(err, msg):
    if err is not None:
        print(f"🔴 Error: {err}")
    else:
        print(f"🟢 Message delivered to {msg.topic()} {msg.value().decode('utf-8')}")

order = {
    'order_id': str(uuid.uuid4()),
    'username': 'test_user',
    'item': 'test_product',
    'quantity': 1
}

for i in range(10):
    producer.produce(topic='orders', value=json.dumps(order).encode('utf-8'), callback=delivery_report)
    producer.produce(topic='notifications', value=json.dumps(order).encode('utf-8'), callback=delivery_report)

    producer.flush() # wait for all messages to be sent to the broker before exiting the program

# producer.flush() is used to wait for all messages to be sent to the broker before exiting the program

print('order messages sent to kafka')
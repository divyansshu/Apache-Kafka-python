import json
import uuid

from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(config)

def delivery_callback(err, msg):
    if err:
        print(f'Delivery Failed: {err}')
    else:
        print(f"Delivered successfully: {msg.value().decode('utf-8')}")
        print(f"Delivered to {msg.topic()}: partition {msg.partition()}: at offset {msg.offset()}")

order = {
    'order_id': str(uuid.uuid4()),
    'user': 'aman',
    'item': 'cold coffee',
    'quantity': 1
}

value = json.dumps(order).encode('utf-8')
producer.produce(topic='order', value=value, callback=delivery_callback)

producer.flush()
import json

from confluent_kafka import Consumer

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracking',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe(['order'])
print('consumer is running and subscribed to topic: order')

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Error: {msg.error()}')
            continue

        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print(f"Received order {order['quantity']} x {order['item']} x {order['user']}")
except KeyboardInterrupt:
    print('\n Stopping consumer')
finally:
    consumer.close()


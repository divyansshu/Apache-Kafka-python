from confluent_kafka import Producer
from faker import Faker
import random
import time
import json

config = {
    'bootstrap.servers': 'localhost:9092'
}

fake = Faker()

topic = 'network-events'

def delivery_callback(err, msg):
    if err:
        print(f'[ERROR] delivery failed: {err}')
    else:
        print(
            f'[OK] topic={msg.topic()} '
            f'partition={msg.partition()} '
            f'key={msg.key().decode()} '
        )

def make_event():
    return {
        'src_ip': fake.ipv4(),
        'dst_ip': fake.ipv4(),
        'src_port': random.randint(1024, 65535),
        'dst_port': random.choice([22,80,443,3306,5432]),
        'protocol': random.choice(['TCP', 'UDP', 'ICMP']),
        'bytes': random.randint(64, 9000),
        'ts': time.time()
    }

def main():
    producer = Producer(config)

    print(f'Sending 20 events to topic: {topic}.... ')
    for _ in range(20):
        event = make_event()

        # key = src_ip, so all traffic from same IP -> same partition
        key = event['src_ip']
        value = json.dumps(event)

        producer.produce(topic=topic, key=key.encode('utf-8'), value=value.encode('utf-8'), callback=delivery_callback)

        # poll() triggers delivery callbacks for already sent messages
        producer.poll(0)
        time.sleep(0.2)

    # block until all queued messages are acknowledged
    print('Flushing....')
    producer.flush()
    print('Done.')

if __name__ == '__main__':
    main()

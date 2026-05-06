import json
import random
import time
from gc import callbacks

from confluent_kafka import Producer
from faker import Faker
import signal

fake = Faker()

TOPIC = 'raw-events'
config = {
    'bootstrap.servers':'localhost:9092'
}

# ports commonly seen in network traffic
COMMON_PORTS = [80, 443, 22, 3306, 5432, 6379, 8080, 8443]
PROTOCOLS = ['TCP', 'UDP', 'ICMP']


def delivery_callback(err, msg):
    if err:
        print(f'[ERROR] delivery failed: {err}')
    else:
        print(
            f'[OK] topic = {msg.topic()} '
            f'partition = {msg.partition()} '
            f'offset = {msg.offset()} '
            f'key = {msg.key().decode()}'
        )

def make_normal_event():
    """Typical low-volume traffic - small bytes, common ports."""
    return {
        'src_ip': fake.ipv4(),
        'dst_ip': fake.ipv4(),
        'src_port': random.randint(1024, 65535),
        'dst_port': random.choice(COMMON_PORTS),
        'protocol': random.choice(PROTOCOLS),
        'bytes': random.randint(64, 1500),
        'duration': round(random.uniform(0.01, 2.0), 3),
        'ts': time.time()
    }

def make_anomalous_event():
    """
    simulates suspicious traffic -
    unusually high bytes, rare ports, very short duration.
    AutoEncoder will flag these as outliers.
    """
    return {
        'src_ip': fake.ipv4(),
        'dst_ip': fake.ipv4(),
        'src_port': random.randint(1024, 65535),
        'dst_port': random.randint(10000, 65535), # rare hight port
        'protocol': "TCP",
        'bytes': random.randint(500_000, 5_000_000), # high payload
        'duration': round(random.uniform(0.001, 0.05), 3), # suspiciously fast
        'ts': time.time()
    }

def make_bad_event():
    """
    Malformed message - missing fields.
    tests dead-letter topic logic in ml_consumer.
    """
    return {
        'src_ip': fake.ipv4(),
        'ts': time.time()
        # intentionally missing: dst_ip, src_port, dst_port, protocol, bytes, duration
    }

def main():
    producer = Producer(config)

    # graceful shutdown
    running = True
    def shutdown(sig, frame):
        nonlocal running
        running = False
        print('\n [shutdown] stopping producer...')
    signal.signal(signal.SIGINT, shutdown)

    print('Producing network events...(ctrl + C) to stop\n')

    msg_count = 0

    while running:
        # traffic distribution
        # 80% normal, 15% anomalous, 5% malformed
        roll = random.random()
        if roll < 0.80:
            event = make_normal_event()
            label = 'normal'
        elif roll < 0.95:
            event = make_anomalous_event()
            label = 'ANOMALY'
        else:
            event = make_bad_event()
            label = 'BAD'

        key = event['src_ip']
        value = json.dumps(event)

        p.produce(
            topic=TOPIC,
            key = key.encode('utf-8'),
            value = value.encode('utf-8'),
            callback = delivery_callback
        )
        producer.poll(0)

        msg_count += 1
        print(f'[{msg_count:>4}] sent [{label:<7}] src_ip={event["src_ip"]}')

        time.sleep(0.5) # one event every 0.5 sec - easy to watch in real time

    producer.flush()
    print(f'\n[DONE] sent {msg_count} events total')

if __name__ == '__main__':
    main()


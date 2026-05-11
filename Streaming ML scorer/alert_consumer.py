import json
import signal
from confluent_kafka import Consumer, KafkaError

TOPIC = 'alerts'
GROUP_ID = 'anomaly-reader'

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
}

def main():
    print(f'\n [STARTUP] loading consumer...')
    consumer = Consumer(config)

    consumer.subscribe([TOPIC])
    print(f'\n Subscribed to {TOPIC} as group {GROUP_ID}. Waiting for messages....')

    # graceful shutdown
    running = True
    def shutdown(sig, frame):
        nonlocal running
        running = False
        print(f'\n [SHUTDOWN] stopping alert_consumer.py')
    signal.signal(signal.SIGINT, shutdown)

    msg_count = 0
    while running:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'[EOF] partition {(msg.partition())} offset {msg.offset()}')
            else:
                print(f'[ERROR] {msg.error()}')
            continue

        msg_count += 1
        alerts = json.loads(msg.value().decode('utf-8'))
        print(
            f'\n[ALERT #{msg_count}]'
            f'\n src_ip = {alerts["src_ip"]}',
            f"\n dst_ip = {alerts["dst_ip"]}",
            f"\n src_port = {alerts["src_port"]}",
            f"\n dst_port = {alerts["dst_port"]}",
            f"\n protocol = {alerts["protocol"]}",
            f"\n bytes = {alerts["bytes"]}",
            f"\n duration = {alerts["duration"]}",
            f"\n ts = {alerts['ts']}",
            f'\n anomaly_score = {alerts["anomaly_score"]}',
            f'\n detected_at = {alerts["detected_at"]}'
            )

    consumer.close()
    print(f'\n Logged {msg_count} anomalies in terminal')


if __name__ == "__main__":
    main()

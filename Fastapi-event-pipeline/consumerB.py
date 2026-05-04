import json
from confluent_kafka import Consumer, KafkaError
import signal

TOPIC = 'Events'
GROUP_ID = 'event-counter'

def main():
    consumer = Consumer(
        {
            'bootstrap.servers':'localhost:9092',
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest'
        }
    )

    consumer.subscribe([TOPIC])
    print(f"Subscribe to {TOPIC} as group {GROUP_ID}. Waiting ofr messages...")

    running = True
    def shutdown(sign, fame):
        nonlocal running
        running = False
        print(f'\n [SHUTDOWN] stopping consumer..')
    signal.signal(signal.SIGINT, shutdown)

    msg_count = 0
    counter = {}
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

        key = msg.key().decode('utf-8') if msg.key() else None
        value = json.loads(msg.value().decode('utf-8'))

        counter[key] = counter.get(key, 0) + 1
        print(f'\n [COUNTS] { {k: v for k ,v in sorted(counter.items())} }')

        msg_count+= 1
        print(
            f'[{msg_count:>4}] '
            f'partition = {msg.partition()} '
            f'offset = {msg.offset()} '
            f'key = {key} | '
            f'user_id = {value["user_id"]} '
            f'action = {value["action"]}'
        )

    consumer.close()
    print(f'\n [DONE] logged {msg_count} events')


if __name__ == '__main__':
    main()

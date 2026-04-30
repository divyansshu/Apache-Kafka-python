import json
import signal

from confluent_kafka import Consumer
from confluent_kafka.cimpl import KafkaError

TOPIC = 'network-events'
GROUP_ID = 'hello-consumer-1'
def main():
    consumer = Consumer({
        'bootstrap.servers':'localhost:9092',
        'group.id':GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([TOPIC])
    print(f'Subscribed to {TOPIC} as group {GROUP_ID}. Waiting for messages...\n')

    # graceful shutdown on ctrl+c
    running = True
    def shutdown(sig, frame):
        nonlocal running
        print('\nShutting down...')
        running=False
    signal.signal(signal.SIGINT, shutdown)

    msg_count = 0
    while running:
        msg = consumer.poll(timeout=1.0) # block up to 1s waiting for a message

        if msg is None:
            continue # no message in this 1 sec window, loop again

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:

                # reached end of a partition - not an error just info
                print(f'[EOF] partition {(msg.partition())} offset {msg.offset()}')
            else:
                print(f'[ERROR] {msg.error()}')

            continue

        # decode key and value
        key = msg.key().decode('utf-8') if msg.key() else None
        event = json.loads(msg.value().decode('utf-8'))

        msg_count+= 1
        print(
            f'[{msg_count:>3}]  '
            f'partition={msg.partition()} offset={msg.offset()}   '
            f'key={key} |  '
            f"{event['src_ip']}:{event['src_port']} ->   "
            f"{event['dst_ip']}: {event['dst_port']}    "
            f"proto={event['protocol']} bytes={event['bytes']}"
        )

    consumer.close()
    print(f'\consumed {msg_count} messages total')

if __name__ == '__main__':
    main()
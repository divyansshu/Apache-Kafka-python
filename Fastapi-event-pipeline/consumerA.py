import signal
from confluent_kafka import Consumer, KafkaError
import json

#------config------
TOPIC = 'Events'
GROUP_ID = 'event-logger'
LOG_FILE = 'events.log'

#------main-----
def main():
    consumer = Consumer(
        {'bootstrap.servers': 'localhost:9092',
         'group.id': GROUP_ID,
         'auto.offset.reset': 'earliest'
         })

    consumer.subscribe([TOPIC])
    print(f'Subscribed to {TOPIC} as group {GROUP_ID}. Waiting for messages.... (ctrl + C) to stop..\n')

#----graceful shutdown-------
    running = True
    def shutdown(sign, frame):
        nonlocal running
        running = False
        print(f'\n [Shutdown] stopping consumer..')
    signal.signal(signal.SIGINT, shutdown)

#----poll loop-------
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

        #-----decode---------
        key = msg.key().decode('utf-8') if msg.key() else None
        value = json.loads(msg.value().decode('utf-8'))

        #----write to a log file-------
        with open(LOG_FILE, 'a') as f:
           f.write(json.dumps(value) + "\n")

        msg_count += 1
        print(
            f'[{msg_count:>4}] '
            f'partition={msg.partition()} '
            f'offset={msg.offset()} '
            f'key={key} | '
            f'user_id={value["user_id"]} '
            f'action={value["action"]}'
        )

    #-----cleanup-------
    consumer.close()
    print(f'\n[DONE] Logged {msg_count} events to {LOG_FILE}')
if __name__ == '__main__':
    main()
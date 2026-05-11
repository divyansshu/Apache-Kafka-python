from confluent_kafka import  Consumer, Producer, KafkaError
import joblib
import json
import signal
import numpy as np
import time

TOPIC_INPUT = 'raw_events'
TOPIC_ALERTS = 'alerts'
TOPIC_DEAD_LETTERS = 'dead-letters'
GROUP_ID = 'ml-scorer'

# 1. Configuration for both Consumer and Producer
config = {'bootstrap.servers': 'localhost:9092'}

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

def produce_alert(producer: Producer, event:dict, score:float):
    alert = {
        'src_ip': event['src_ip'],
        "dst_ip": event["dst_ip"],
        "src_port": event["src_port"],
        "dst_port": event["dst_port"],
        "protocol": event["protocol"],
        "bytes": event["bytes"],
        "duration": event["duration"],
        "ts": event["ts"],
        'anomaly_score': score,
        'detected_at': time.time()
    }
    producer.produce(
        topic=TOPIC_ALERTS,
        key=event['src_ip'].encode('utf-8'),
        value=json.dumps(alert).encode('utf-8'),
        callback=delivery_callback
    )
    producer.poll(0)

def extract_features(event: dict):

     features = np.array([[
     event['bytes'],
     event['duration'],
     event['src_port'],
     event['dst_port'],
     ]], dtype=float)
     return features

def producer_dead_letter(producer: Producer, msg, reason: str):
    dead_letter = {
        'reason': reason,
        'raw_message': msg.value().decode('utf-8', errors='replace'),
        'topic': msg.topic(),
        'partition': msg.partition(),
        'offset': msg.offset(),
        'ts': time.time()
    }
    producer.produce(
        topic=TOPIC_DEAD_LETTERS,
        key=str(msg.partition()).encode('utf-8'),
        value=json.dumps(dead_letter).encode('utf-8'),
        callback=delivery_callback
    )
    producer.poll(0)

def main():
    print(f'[STARTUP] loading model - model.pkl')
    model = joblib.load('model.pkl')
    print('[STARTUP] model loaded')

    # creat consumer
    consumer = Consumer(
        {
            **config,
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False # manual commit only
        }
    )

    # create producer
    producer = Producer(config)

    consumer.subscribe([TOPIC_INPUT])
    print(f'Subscribed to {TOPIC_INPUT} as group {GROUP_ID}. Waiting for network traffic...\n')

    # graceful shutdown
    running = True
    def shutdown(sig, frame):
        nonlocal running
        running = False
        print(f'\n [SHUTDOWN] stopping ml_consumer..')
    signal.signal(signal.SIGINT, shutdown)

    # counters
    total = 0
    normal = 0
    anomalies = 0
    dead = 0

    # poll loop
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

        total += 1

        # step 1: Decode json
        try:
            event = json.loads(msg.value().decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f'[DEAD] decode error: {e}')
            producer_dead_letter(producer, msg, reason=f'decode error: {e}')
            consumer.commit(msg)
            dead += 1
            continue

        # step 2: extract features
        try:
            features = extract_features(event)
        except KeyError as e:
            print(f'[DEAD] missing field: {e}')
            producer_dead_letter(producer, msg, reason=f'missing field: {e}')
            consumer.commit(msg)
            dead += 1
            continue
        except (ValueError, TypeError) as e:
            print(f'[DEAD] bad field type: {e}')
            producer_dead_letter(producer, msg, reason=f'bad field type: {e}')
            consumer.commit(msg)
            dead += 1
            continue

        # step 3: score with model
        prediction = model.predict(features)[0] # 1 or -1
        score = model.decision_function(features)[0] # continuous score

        # step 4: route based on prediction
        if prediction == -1:
            anomalies += 1
            print(
                f'[ANOMALY] src_ip={event["src_ip"]} '
                f'bytes={event["bytes"]} '
                f'duration={event["duration"]} '
                f'score={score:.4f}'
            )
            produce_alert(producer, event, score)
        else:
            normal += 1
            print(
                f'[normal] src_ip={event["src_ip"]} '
                f'bytes={event["bytes"]} '
                f'duration={event["duration"]} '
                f'score={score:.4f}'
            )

        # step 5: commit offset after everything above
        consumer.commit(msg)

    # cleanup
    producer.flush()
    consumer.close()
    print(f'\n [Done] processed={total} normal={normal} anomalies={anomalies} dead={dead}')

if __name__ == '__main__':
    main()
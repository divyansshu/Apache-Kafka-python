from confluent_kafka import Consumer

if __name__ == "__main__":
    
    config = {
        'bootstrap.servers': 'localhost:57845',
        
        'group.id': 'kafka-python-getting-started',
        'auto.offset.reset': 'earliest'
    }
    
    # create consumer instance
    consumer = Consumer(config)
    
    # subscribe to topic
    topic = 'purchases'
    consumer.subscribe([topic])
    
    # poll for new messages from kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # initial message consumption may take up to `session.timeout.ms` for the consumer group to rebalance and start consuming
                print('waiting...')
            elif msg.error():
                print('Error: %s'.format(msg.error()))
            else:
                # extract the (optional) key, value and print
                print('consumed event from topic {topic}: key = {key:12} value: {value:12}'.format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')
                ))
    except KeyboardInterrupt:
        pass
    finally:
        # leave group and commit final reports
        consumer.close()
                
            
    
    
    
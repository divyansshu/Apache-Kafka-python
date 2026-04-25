from random import choice
from confluent_kafka import Producer

if __name__ == "__main__":
    
    config = {
        # user specific properties that you must set
        'bootstrap.servers': 'localhost:57845',
        
        # fixed properties
        'acks': 'all'
    }
    
    # create producer instance
    producer = Producer(config)
    
    # optional per-message delivery callback (trigerred by poll() or flush())
    # when a message has been successfully delivered or permanently failed delivery(after retries)
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message Failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    
    # produce data by selecting random values from these lists.
    topic = 'purchases'
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']
    
    count = 0
    for _ in range(10):
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, user_id, product, callback=delivery_callback)
        count += 1
        
        # trigger any outstanding delivery report callbacks.
        producer.poll(0)
    
    # block until the messages are delivered.
    producer.flush()
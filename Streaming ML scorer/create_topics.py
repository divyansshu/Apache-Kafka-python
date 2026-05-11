from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({
    'bootstrap.servers': 'localhost:9092'
})

topics = [
    NewTopic('raw_events', num_partitions=3, replication_factor=1),
    NewTopic('alerts', num_partitions=1, replication_factor=1),
    NewTopic('dead-letters', num_partitions=1, replication_factor=1)
]

# create_topics returns a dict of {topic_name: Future}
futures = admin.create_topics(topics)

for name, future in futures.items():
    try:
        future.result() # blocks until topic is created or raises
        print(f'[OK] topic {name} created')
    except Exception as e:
        print(f'[ERROR] failed to create topics {name}: {e}')
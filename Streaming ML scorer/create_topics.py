from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({
    'bootstrap.servers': 'localhost:9092'
})

topics = [
    NewTopic('raw_events', num_partitions=3, replication_factor=1),
    NewTopic('alerts', num_partitions=1, replication_factor=1),
    NewTopic('dead-letters', num_partitions=1, replication_factor=1)
]

admin.create_topics(topics)
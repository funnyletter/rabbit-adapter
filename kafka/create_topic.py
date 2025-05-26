from confluent_kafka.admin import AdminClient, NewTopic

def create_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    fs = admin_client.create_topics([topic])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic}' created")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")

if __name__ == "__main__":
    create_topic("rabbit_queue", num_partitions=1, replication_factor=1)

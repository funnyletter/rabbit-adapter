from confluent_kafka import Consumer, KafkaError
import sys

def main():
    # Create a Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "my_group",
        "auto.offset.reset": "earliest"
    })

    # Subscribe to a topic
    consumer.subscribe(["rabbit_queue"])
    print("Waiting for messages... Press Ctrl+C to exit.")

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Process the message
            print(f"Received message: {msg.value().decode("utf-8")}")
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        # Close the consumer
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()

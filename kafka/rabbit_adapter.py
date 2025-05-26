import pika
from confluent_kafka import Producer
from loguru import logger



def get_kafka_producer():
    """Create and return a Kafka producer instance."""
    logger.info("Creating Kafka producer")
    return Producer({"bootstrap.servers": "localhost:9092"})


def get_rabbitmq_consumer(queue_name: str):
    """Create and return a RabbitMQ consumer instance."""
    logger.info("Creating RabbitMQ consumer for queue: {}", queue_name)
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return connection, channel


def kafka_delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.error("Message delivery failed: {}".format(err))
    else:
        logger.info("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def send_message_to_kafka(producer, topic, message):
    """Send a message to a Kafka topic."""
    logger.info("Sending message to Kafka topic: {}", topic)
    producer.produce(topic, value=message, callback=kafka_delivery_report)
    producer.poll(0)  # Trigger delivery report callbacks
    logger.info("Message sent: {}", message)


def main():
    """Main function to set up RabbitMQ consumer and Kafka producer."""
    # Create Kafka producer
    kafka_producer = get_kafka_producer()

    # Create RabbitMQ consumer
    rabbitmq_connection, rabbitmq_channel = get_rabbitmq_consumer("hello")

    def callback(ch, method, properties, body):
        """Callback function to handle incoming RabbitMQ messages."""
        message = body.decode('utf-8')
        logger.info("Received message from RabbitMQ: {}", message)
        send_message_to_kafka(kafka_producer, "rabbit_queue", message)

    # Start consuming messages from RabbitMQ
    rabbitmq_channel.basic_consume(queue="hello", on_message_callback=callback, auto_ack=True)
    logger.info("Waiting for messages from RabbitMQ. To exit press CTRL+C")
    
    try:
        rabbitmq_channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Exiting...")
        rabbitmq_connection.close()
        kafka_producer.flush()  # Ensure all messages are sent before exiting


if __name__ == "__main__":
    main()

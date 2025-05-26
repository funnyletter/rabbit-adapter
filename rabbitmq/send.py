#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.queue_declare(queue="hello")

content = ["Hello World!", "Hello RabbitMQ!", "Hello Python!"]
i = 0

for message in content:
    i += 1
    channel.basic_publish(
        exchange="",
        routing_key="hello",
        body=message
    )
    print(f" [x] Sent '{message}' ({i}/{len(content)})")

connection.close()

__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import pika


def establish_rabbitmq_connection(rabbitmq_uri):
    """
    What it says on the tin.

    Input:  - rabbitmq_uri: A RabbitMQ URI.

    Output: - connection: A RabbitMQ connection.
    """
    connection_parameters = pika.URLParameters(rabbitmq_uri)

    connection = pika.BlockingConnection(connection_parameters)
    return connection


def simple_notification(connection, queue_name, exchange_name, text_body):
    """
    Publishes a simple notification.

    Inputs: - connection: A rabbitmq connection object.
            - queue_name: The name of the queue to be checked or created.
            - exchange_name: The name of the notification exchange.
            - text_body: The text to be published.
    """
    channel = connection.channel()

    channel.queue_declare(queue=queue_name)

    channel.basic_publish(exchange=exchange_name,
                          routing_key=queue_name,
                          body=text_body)

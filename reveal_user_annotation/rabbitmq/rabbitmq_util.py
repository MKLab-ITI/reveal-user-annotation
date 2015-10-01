__author__ = 'Georgios Rizos (georgerizos@iti.gr)'

import sys
import subprocess
import urllib
from amqp import Connection, Message


if sys.version_info > (3,):
    import urllib.parse as urlparse
else:
    import urlparse


def translate_rabbitmq_url(url):
    if url[0:4] == "amqp":
        url = "http" + url[4:]

    parts = urlparse.urlparse(url)

    if parts.scheme == "https":
        ssl = True
    else:
        ssl = False

    if parts.hostname is not None:
        host_name = parts.hostname
    else:
        host_name = "localhost"

    if parts.port is not None:
        port = int(parts.port)
    else:
        port = 5672

    if parts.username is not None:
        user_name = parts.username
        password = parts.password
    else:
        user_name = parts.username
        password = parts.password

    path_parts = parts.path.split('/')
    virtual_host = urllib.parse.unquote(path_parts[1])
    virtual_host = virtual_host  # May be an empty path if URL is e.g. "amqp://guest:guest@localhost:5672/vhost"
    if virtual_host == "":
        virtual_host = "/"  # Default vhost

    return user_name, password, host_name, port, virtual_host, ssl


def establish_rabbitmq_connection(rabbitmq_uri):
    """
    What it says on the tin.

    Input:  - rabbitmq_uri: A RabbitMQ URI.

    Output: - connection: A RabbitMQ connection.
    """
    userid, password, host, port, virtual_host, ssl = translate_rabbitmq_url(rabbitmq_uri)

    connection = Connection(userid=userid,
                            password=password,
                            host=host,
                            port=port,
                            virtual_host=virtual_host,
                            ssl=False)

    return connection


def simple_notification(connection, queue_name, exchange_name, routing_key, text_body):
    """
    Publishes a simple notification.

    Inputs: - connection: A rabbitmq connection object.
            - queue_name: The name of the queue to be checked or created.
            - exchange_name: The name of the notification exchange.
            - routing_key: The routing key for the exchange-queue binding.
            - text_body: The text to be published.
    """
    channel = connection.channel()
    channel.queue_declare(queue_name, durable=True)
    channel.exchange_declare(exchange_name, type='direct', durable=True)
    channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

    message = Message(text_body)
    channel.basic_publish(message, exchange_name, routing_key)


def simpler_notification(channel, queue_name, exchange_name, routing_key, text_body):
    message = Message(text_body)
    channel.basic_publish(message, exchange_name, routing_key)


def rabbitmq_server_service(command):
    subprocess.call(["service", "rabbitmq-server", command])

import os
import click
import logging
import json
from libs.rabbitmq import create_rabbitmq_channel
from libs.logging import init_logging

logger = logging.getLogger(__name__)

class EventPrinter:
    def __init__(self, n=100, verbose=False):
        self.__counter = 0
        self.__verbose = verbose
        self.__n = n

    def __call__(self, ch, method, properties, body):
        if self.__counter % self.__n == 0:
            if self.__verbose:
                logger.info("received %s %s %s", self.__counter, method.routing_key, body)
            else:
                logger.info("received %s %s", self.__counter, method.routing_key)
        self.__counter = self.__counter + 1

@click.command()
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
@click.option('--binding-keys', default=lambda: os.environ.get('RABBITMQ_BINDING_KEYS', '#'), required=True, help='RabbitMQ binding keys ($RABBITMQ_BINDING_KEYS, "#")')
@click.option('--queue-name', default=lambda: os.environ.get('RABBITMQ_QUEUE_NAME', ''), required=True, help='RabbitMQ queue name ($RABBITMQ_QUEUE_NAME, "")')
@click.option('--n', default=1, type=int, required=True, help='Print a line every records')
@click.option('--verbose', default=False, is_flag=True, required=True, help='Print verbose output')
def rabbitmq_to_stdout(rabbitmq_url, rabbitmq_exchange, binding_keys, queue_name, n, verbose):
    init_logging()
    logger.info('trying to open rabbitmq channel')
    rabbitmq_channel = create_rabbitmq_channel(rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    result = rabbitmq_channel.queue_declare(queue_name, durable=True, exclusive=True, auto_delete=True)
    queue_name = result.method.queue
    for binding_key in binding_keys.split(','):
        logger.info('binding to exchange %s, queue %s, binding_key %s', rabbitmq_exchange, queue_name, binding_key)
        rabbitmq_channel.queue_bind(exchange=rabbitmq_exchange, queue=queue_name, routing_key=binding_key)

    rabbitmq_channel.basic_consume(queue=queue_name, on_message_callback=EventPrinter(n=n, verbose=verbose), auto_ack=True)
    rabbitmq_channel.start_consuming()

if __name__ == '__main__':
    rabbitmq_to_stdout()

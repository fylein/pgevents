import os
import signal
import click

from common import log
from common.event import BaseEvent
from common.qconnector import RabbitMQConnector
from consumer.event_consumer import EventConsumer

logger = log.get_logger(__name__)


@click.command()
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
@click.option('--binding-keys', default=lambda: os.environ.get('RABBITMQ_BINDING_KEYS', '#'), required=True, help='RabbitMQ binding keys ($RABBITMQ_BINDING_KEYS, "#")')
@click.option('--queue-name', default=lambda: os.environ.get('RABBITMQ_QUEUE_NAME', ''), required=True, help='RabbitMQ queue name ($RABBITMQ_QUEUE_NAME, "")')
def consume(rabbitmq_url, rabbitmq_exchange, binding_keys, queue_name):
    event_logger = EventConsumer(
        qconnector_cls=RabbitMQConnector,
        event_cls=BaseEvent,
        rabbitmq_url=rabbitmq_url,
        rabbitmq_exchange=rabbitmq_exchange,
        queue_name=queue_name,
        binding_keys=binding_keys
    )

    signal.signal(signal.SIGTERM, event_logger.shutdown)
    signal.signal(signal.SIGINT, event_logger.shutdown)

    event_logger.connect()
    event_logger.start_consuming()

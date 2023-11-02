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
@click.option('--pg_host', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pg_port', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pg_database', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pg_user', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pg_password', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pg-output-plugin', default=lambda: os.environ.get('PGOUTPUTPLUGIN', 'wal2json'), required=True, help='Postgresql Output Plugin ($PGOUTPUTPLUGIN)')
def consume(rabbitmq_url, rabbitmq_exchange, binding_keys, queue_name, pg_host, pg_port, pg_database, pg_user, pg_password, pg_output_plugin):
    event_logger = EventConsumer(
        qconnector_cls=RabbitMQConnector,
        event_cls=BaseEvent,
        rabbitmq_url=rabbitmq_url,
        rabbitmq_exchange=rabbitmq_exchange,
        queue_name=queue_name,
        binding_keys=binding_keys,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_database=pg_database,
        pg_user=pg_user,
        pg_password=pg_password,
        pg_output_plugin=pg_output_plugin
    )

    signal.signal(signal.SIGTERM, event_logger.shutdown)
    signal.signal(signal.SIGINT, event_logger.shutdown)

    event_logger.connect()
    event_logger.start_consuming()

import os
import signal

import click

from common import log
from common.event import BaseEvent
from common.qconnector import RabbitMQConnector
from producer.event_producer import EventProducer

logger = log.get_logger(__name__)


@click.command()
@click.option('--pg_host', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pg_port', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pg_database', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pg_user', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pg_password', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pg_replication_slot', default=lambda: os.environ.get('PGREPLICATIONSLOT', None), required=True, help='Postgresql Replication Slot Name ($PGREPLICATIONSLOT)')
@click.option('--pg_output_plugin', default=lambda: os.environ.get('PGOUTPUTPLUGIN', 'wal2json'), required=True, help='Postgresql Output Plugin ($PGOUTPUTPLUGIN)')
@click.option('--pg_tables', default=lambda: os.environ.get('PGTABLES', None), required=False, help='Restrict to specific tables e.g. public.transactions,public.reports')
@click.option('--pg_publication_name', default=lambda: os.environ.get('PGPUBLICATION', None), required=False, help='Restrict to specific publications e.g. events')
@click.option('--rabbitmq_url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq_exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
def produce(pg_host, pg_port, pg_database, pg_user, pg_password, pg_replication_slot, pg_output_plugin, pg_tables, pg_publication_name, rabbitmq_url, rabbitmq_exchange):
    p = EventProducer(
        qconnector_cls=RabbitMQConnector,
        event_cls=BaseEvent,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_database=pg_database,
        pg_user=pg_user,
        pg_password=pg_password,
        pg_replication_slot=pg_replication_slot,
        pg_output_plugin=pg_output_plugin,
        pg_tables=pg_tables,
        pg_publication_name=pg_publication_name,
        rabbitmq_url=rabbitmq_url,
        rabbitmq_exchange=rabbitmq_exchange
    )

    def signal_handler(signum, frame):
        p.shutdown()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    p.connect()
    p.start_consuming()

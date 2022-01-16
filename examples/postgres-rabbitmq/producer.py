import log
import os
import signal

import click
from fyle_pgevents.event import BaseEvent

from fyle_pgevents.qconnector import RabbitMQConnector
from fyle_pgevents.producers import PGEventProducer

logger = log.get_logger(__name__)


class Producer(PGEventProducer):
    def get_event_routing_key_and_event(self, table_name: str, event: BaseEvent) -> (str, BaseEvent):
        return table_name, event


@click.command()
@click.option('--pg_host', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pg_port', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pg_database', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pg_user', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pg_password', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pg_replication_slot', default=lambda: os.environ.get('PGREPLICATIONSLOT', None), required=True, help='Postgresql Replication Slot Name ($PGREPLICATIONSLOT)')
@click.option('--pg_tables', default=lambda: os.environ.get('PGTABLES', None), required=False, help='Restrict to specific tables e.g. public.transactions,public.reports')
@click.option('--rabbitmq_url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq_exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
def producer(pg_host, pg_port, pg_database, pg_user, pg_password, pg_replication_slot, pg_tables, rabbitmq_url, rabbitmq_exchange):
    p = Producer(
        qconnector_cls=RabbitMQConnector,
        event_cls=BaseEvent,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_database=pg_database,
        pg_user=pg_user,
        pg_password=pg_password,
        pg_replication_slot=pg_replication_slot,
        pg_tables=pg_tables,
        rabbitmq_url=rabbitmq_url,
        rabbitmq_exchange=rabbitmq_exchange
    )

    signal.signal(signal.SIGTERM, p.shutdown)
    signal.signal(signal.SIGINT, p.shutdown)

    p.connect()
    p.start_consuming()

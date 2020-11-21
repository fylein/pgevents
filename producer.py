import logging
import os
import signal

import click

from common.logging import init_logging
from common.pgevent_producer import PGEventProducer

logger = logging.getLogger(__name__)

@click.command()
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pgport', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pgdatabase', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pguser', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pgpassword', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pgslot', default=lambda: os.environ.get('PGSLOT', None), required=True, help='Postgresql Replication Slot Name ($PGSLOT)')
@click.option('--pgtables', default=lambda: os.environ.get('PGTABLES', None), required=False, help='Restrict to specific tables e.g. public.transactions,public.reports')
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
def producer(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, pgtables, rabbitmq_url, rabbitmq_exchange):
    init_logging()
    p = PGEventProducer(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, 
            pgslot=pgslot, pgtables=pgtables, rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    signal.signal(signal.SIGTERM, p.shutdown)
    signal.signal(signal.SIGINT, p.shutdown)
    p.process()

if __name__ == '__main__':
    producer()

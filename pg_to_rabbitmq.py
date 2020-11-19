import os
import json
import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection
import pika
import click
import logging
from libs.msg import consume_stream
from libs.pg import create_db_cursor
from libs.rabbitmq import create_rabbitmq_channel
from libs.logging import init_logging

logger = logging.getLogger(__name__)

def process_event_rabbitmq(rabbitmq_exchange, rabbitmq_channel, event):
    routing_key = event['routing_key']
    body = json.dumps(event, sort_keys=True, default=str)
    logger.debug('sending routing_key %s body %s ', routing_key, body)
    rabbitmq_channel.basic_publish(exchange=rabbitmq_exchange, routing_key=routing_key, body=body)

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
def pg_to_rabbitmq(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, pgtables, rabbitmq_url, rabbitmq_exchange):
    init_logging()
    db_cur = create_db_cursor(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgslot=pgslot, pgtables=pgtables)
    rabbitmq_channel = create_rabbitmq_channel(rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    rabbitmq_sender_fn = lambda event: process_event_rabbitmq(rabbitmq_exchange=rabbitmq_exchange, rabbitmq_channel=rabbitmq_channel, event=event)
    db_cur.consume_stream(consume=lambda msg : consume_stream(msg=msg, process_event_fn=rabbitmq_sender_fn))

if __name__ == '__main__':
    pg_to_rabbitmq()

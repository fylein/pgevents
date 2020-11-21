import json
import logging
import os
import signal
import threading
import time

import click
import pika
import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection

from common.logging import init_logging
from common.msg import msg_to_event
from common.decorators import retry

logger = logging.getLogger(__name__)

class Producer:
    def __init__(self, pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, pgtables, rabbitmq_url, 
                rabbitmq_exchange):
        self.__pghost = pghost
        self.__pgport = pgport
        self.__pgdatabase = pgdatabase
        self.__pguser = pguser
        self.__pgpassword = pgpassword
        self.__pgslot = pgslot
        self.__pgtables = pgtables
        self.__rabbitmq_url = rabbitmq_url
        self.__rabbitmq_exchange = rabbitmq_exchange
        self.__db_conn = None
        self.__db_cur = None
        self.__rmq_conn = None
        self.__rmq_channel = None

    @retry(n=3, backoff=15, exceptions=(psycopg2.errors.ObjectInUse, psycopg2.InterfaceError))
    def __connect_db(self):
        self.__db_conn = psycopg2.connect(host=self.__pghost, port=self.__pgport, dbname=self.__pgdatabase, user=self.__pguser, 
                                password=self.__pgpassword, connection_factory=LogicalReplicationConnection)
        self.__db_cur = self.__db_conn.cursor()
        self.__create_slot()

    def __disconnect_db(self):
        if self.__db_cur:
            self.__db_cur.close()
        if self.__db_conn:
            self.__db_conn.close()

    @retry(n=3, backoff=15, exceptions=(pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError))
    def __connect_rabbitmq(self):
        self.__rmq_conn = pika.BlockingConnection(pika.URLParameters(self.__rabbitmq_url))
        self.__rmq_channel = self.__rmq_conn.channel()
        self.__rmq_channel.exchange_declare(exchange=self.__rabbitmq_exchange, exchange_type='topic')

    def __disconnect_rabbitmq(self):
        if self.__rmq_channel:
            self.__rmq_channel.close()
        if self.__rmq_conn:
            self.__rmq_conn.close()

    def __create_slot(self):
        logger.debug('trying to create replication slot %s', self.__pgslot)
        try:
            self.__db_cur.create_replication_slot(slot_name=self.__pgslot, slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                                output_plugin='wal2json')
        except psycopg2.ProgrammingError as err:
            if err.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
                raise
            else:
                logger.debug('slot already exists, reusing')
        logger.debug('start replication')
        options = {'format-version': 2, 'include-types': True, 'include-lsn': True}
        if self.__pgtables and len(self.__pgtables) > 0:
            options['add-tables'] = self.__pgtables
        logger.debug('options for slot %s', options)
        self.__db_cur.start_replication(slot_name=self.__pgslot, options=options, decode=True)
        logger.debug('started consuming')

    def __drop_slot(self):
        self.__db_cur.drop_replication(slot_name=self.__pgslot)

    def __send_event(self, event):
        routing_key = event['tablename']
        body = json.dumps(event, sort_keys=True, default=str)
        logger.debug('sending routing_key %s body %s ', routing_key, body)
        self.__rmq_channel.basic_publish(exchange=self.__rabbitmq_exchange, routing_key=routing_key, body=body)

    def __consume_stream(self, msg):
        event = msg_to_event(self.__pgdatabase, msg)
        if event:
            self.__send_event(event=event)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def process(self):
        self.__connect_db()
        self.__connect_rabbitmq()
        logger.info('connected to db and rabbitmq successfully')

        while True:
            try:
                self.__db_cur.consume_stream(self.__consume_stream)
            except (psycopg2.errors.ObjectInUse, psycopg2.InterfaceError) as e:
                logger.exception('hit unexpected exception while processing, will backoff and reconnect...', e)
                self.__connect_db()

            except (pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError) as e:
                logger.exception('hit unexpected exception while processing, will backoff and reconnect...', e)
                self.__connect_rabbitmq()

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
    p = Producer(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgslot=pgslot, pgtables=pgtables, rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    p.process()

if __name__ == '__main__':
    producer()

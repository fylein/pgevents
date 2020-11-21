import json
import logging
import os

import click
import pika
import psycopg2
from psycopg2.extras import Json

from common.logging import init_logging

logger = logging.getLogger(__name__)

# create table audit_tmp (action varchar(2), new jsonb, old jsonb, diff jsonb, tablename text, id text)
class Consumer:
    def __init__(self, pghost, pgport, pgdatabase, pguser, pgpassword, pgaudittable, rabbitmq_url, 
                rabbitmq_exchange, queue_name, binding_keys):
        self.__pghost = pghost
        self.__pgport = pgport
        self.__pgdatabase = pgdatabase
        self.__pguser = pguser
        self.__pgpassword = pgpassword
        self.__pgaudittable = pgaudittable
        self.__rabbitmq_url = rabbitmq_url
        self.__rabbitmq_exchange = rabbitmq_exchange
        self.__queue_name = queue_name
        self.__binding_keys = binding_keys
        self.__db_conn = None
        self.__db_cur = None
        self.__rmq_conn = None
        self.__rmq_channel = None
        self.__insert_statement = f'insert into {pgaudittable} (action, new, old, diff, tablename, id, updated_at, updated_by) values (%(action)s, %(new)s, %(old)s, %(diff)s, %(tablename)s, %(id)s, %(updated_at)s, %(updated_by)s)'


    @retry(n=3, backoff=15, exceptions=(psycopg2.errors.ObjectInUse, psycopg2.InterfaceError))
    def __connect_db(self):
        self.__db_conn = psycopg2.connect(host=self.__pghost, port=self.__pgport, dbname=self.__pgdatabase, user=self.__pguser, 
                                password=self.__pgpassword)
        self.__db_cur = self.__db_conn.cursor()


    @retry(n=3, backoff=15, exceptions=(pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError))
    def __connect_rabbitmq(self):
        self.__rmq_conn = pika.BlockingConnection(pika.URLParameters(self.__rabbitmq_url))
        self.__rmq_channel = self.__rmq_conn.channel()
        self.__rmq_channel.exchange_declare(exchange=self.__rabbitmq_exchange, exchange_type='topic')
# TODO: check these parameters - they might not be correct
        res = __rmq_channel.queue_declare(self.__queue_name, durable=True, exclusive=False, auto_delete=True)

        self.__queue_name = res.method.queue
        for binding_key in self.__binding_keys.split(','):
            logger.info('binding to exchange %s, queue %s, binding_key %s', self.__rabbitmq_exchange, self.__queue_name, binding_key)
            self.__rmq_channel.queue_bind(exchange=self.__rabbitmq_exchange, queue=self.__queue_name, routing_key=binding_key)

    def __process_body(self, ch, method, properties, body):
        event = json.loads(body)
        logger.debug('got event %s', event)
        doc = {
            'action': event['action'],
            'id': event['id'],
            'updated_at': event['updated_at'],
            'new': Json(event['new']),
            'old': Json(event['old']),
            'diff': Json(event['diff']),
            'tablename': event['tablename'],
            'updated_by': Json(event['updated_by'])
        }
        logger.debug('inserting doc %s', doc)
        self.__db_cur.execute(self.__insert_statement, doc)
        self.__db_conn.commit()

    def process(self):
        self.__connect_rabbitmq()
        self.__connect_db()
        logger.info('connected to rabbitmq and audit db successfully')
        while True:
            try:
                self.__rabbitmq_channel.basic_consume(queue=self.__queue_name, on_message_callback=self.__process_body, auto_ack=True)
                self.__rabbitmq_channel.start_consuming()
            except (psycopg2.InterfaceError) as e:
                logger.exception('hit unexpected exception while processing, will backoff and reconnect...', e)
                self.__connect_db()
            except (pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError) as e:
                logger.exception('hit unexpected exception while processing, will backoff and reconnect...', e)
                self.__connect_rabbitmq()

@click.command()
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
@click.option('--binding-keys', default=lambda: os.environ.get('RABBITMQ_BINDING_KEYS', '#'), required=True, help='RabbitMQ binding keys ($RABBITMQ_BINDING_KEYS, "#")')
@click.option('--queue-name', default=lambda: os.environ.get('RABBITMQ_QUEUE_NAME', ''), required=True, help='RabbitMQ queue name ($RABBITMQ_QUEUE_NAME, "")')
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pgport', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pgdatabase', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pguser', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pgpassword', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pgaudittable', default=lambda: os.environ.get('PGAUDITTABLE', None), required=True, help='Postgresql Audit Table ($PGAUDITTABLE)')
def consumer_audit(rabbitmq_url, rabbitmq_exchange, binding_keys, queue_name, pghost, pgport, pgdatabase, pguser, pgpassword, pgaudittable):
    init_logging()
    c = Consumer(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgaudittable=pgaudittable, 
            rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange, binding_keys=binding_keys, queue_name=queue_name)
    c.process()

if __name__ == '__main__':
    consumer_audit()

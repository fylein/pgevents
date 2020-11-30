import logging
import os
import signal

import click
import psycopg2
from psycopg2.extras import Json

from common.decorators import retry
from common.logging import init_logging
from common.pgevent_consumer import PGEventConsumer

logger = logging.getLogger(__name__)

class ConsumerAuditEventProcessor:
    def __init__(self, pghost, pgport, pgdatabase, pguser, pgpassword, pgaudittable):
        self.__pghost = pghost
        self.__pgport = pgport
        self.__pgdatabase = pgdatabase
        self.__pguser = pguser
        self.__pgpassword = pgpassword
        self.__pgaudittable = pgaudittable
        self.__db_conn = None
        self.__db_cur = None
        self.__shutdown = False
        self.__insert_statement = f'insert into {pgaudittable} (action, new, old, diff, tablename, id, updated_at, updated_by) values (%(action)s, %(new)s, %(old)s, %(diff)s, %(tablename)s, %(id)s, %(updated_at)s, %(updated_by)s)'
        self.__connect_db()

    @retry(n=3, backoff=15, exceptions=(psycopg2.OperationalError, psycopg2.InterfaceError))
    def __connect_db(self):
        self.__db_conn = psycopg2.connect(host=self.__pghost, port=self.__pgport, dbname=self.__pgdatabase, user=self.__pguser,
                                          password=self.__pgpassword)
        self.__db_cur = self.__db_conn.cursor()

    def __call__(self, event):
        logger.debug('got event %s', event)
        if event['action'] == 'U' and not event['diff']:
            logger.debug('skipping empty update event')
            return
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
    process_event_fn = ConsumerAuditEventProcessor(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgaudittable=pgaudittable)
    pgevent_consumer = PGEventConsumer(rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange, queue_name=queue_name, binding_keys=binding_keys, process_event_fn=process_event_fn)
    signal.signal(signal.SIGTERM, pgevent_consumer.shutdown)
    signal.signal(signal.SIGINT, pgevent_consumer.shutdown)
    pgevent_consumer.process()

if __name__ == '__main__':
    consumer_audit()

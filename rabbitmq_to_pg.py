import os
import click
import logging
import json
from libs.rabbitmq import create_rabbitmq_channel
from libs.logging import init_logging
import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)

# create table audit_tmp (action varchar(2), new jsonb, old jsonb, diff jsonb, tablename text, id text)
class PGWriter:
    def __init__(self, pghost, pgport, pgdatabase, pguser, pgpassword, pgaudittable):
        self.__connection = psycopg2.connect(user=pguser,
                                        password=pgpassword,
                                        host=pghost,
                                        port=pgport,
                                        database=pgdatabase)
        self.__cursor = self.__connection.cursor()
        self.__insert_statement = f'insert into {pgaudittable} (action, new, old, diff, tablename, id, updated_at) values (%(action)s, %(new)s, %(old)s, %(diff)s, %(tablename)s, %(id)s, %(updated_at)s)'
#        self.__insert_statement = f'insert into {pgaudittable} select * from json_populate_recordset(NULL::{pgaudittable}, %s)'

    def __find_value(self, event, colname):
        cols = event['new']
        if event['action'] == 'D':
            cols = event['old']
        for c in cols:
            if c['name'] == colname:
                return c['value']
        return None

    def __call__(self, ch, method, properties, body):
        event = json.loads(body)
        id = self.__find_value(event, 'id')
        updated_at = self.__find_value(event, 'updated_at')
        doc = {
            'action': event['action'],
            'id': id,
            'updated_at': updated_at,
            'new': Json(event['new']),
            'old': Json(event['old']),
            'diff': Json(event['diff']),
            'tablename': method.routing_key
        }
        logger.info('inserting doc %s', doc)
        self.__cursor.execute(self.__insert_statement, doc)
        self.__connection.commit()


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
def rabbitmq_to_pg(rabbitmq_url, rabbitmq_exchange, binding_keys, queue_name, pghost, pgport, pgdatabase, pguser, pgpassword, pgaudittable):
    init_logging()
    logger.info('trying to open rabbitmq channel')
    rabbitmq_channel = create_rabbitmq_channel(rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    result = rabbitmq_channel.queue_declare(queue_name, durable=True, exclusive=True, auto_delete=True)
    queue_name = result.method.queue
    for binding_key in binding_keys.split(','):
        logger.info('binding to exchange %s, queue %s, binding_key %s', rabbitmq_exchange, queue_name, binding_key)
        rabbitmq_channel.queue_bind(exchange=rabbitmq_exchange, queue=queue_name, routing_key=binding_key)
    pgwriter = PGWriter(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgaudittable=pgaudittable)
    rabbitmq_channel.basic_consume(queue=queue_name, on_message_callback=pgwriter, auto_ack=True)
    rabbitmq_channel.start_consuming()

if __name__ == '__main__':
    rabbitmq_to_pg()

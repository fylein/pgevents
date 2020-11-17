import os
import json
import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection
import pika
import click
import logging
import re

logger = logging.getLogger(__name__)

def insert_event(change):
    event = {
        'kind': 'insert',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['columnnames'], change['columntypes'])),
        'old': None,
        'new': dict(zip(change['columnnames'], change['columnvalues']))
    }
    return event

def update_event(change):
    event = {
        'kind': 'update',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['columnnames'], change['columntypes'])),
        'old': dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues'])),
        'new': dict(zip(change['columnnames'], change['columnvalues']))
    }
    return event

def delete_event(change):
    event = {
        'kind': 'delete',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keytypes'])),
        'old': dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues'])),
        'new': None
    }
    return event

def msg_to_events_generator(msg):
    obj = json.loads(msg.payload)
    if 'change' in obj:
        for change in obj['change']:
            event = None
            if change['kind'] == 'insert':
                event = insert_event(change)
            elif change['kind'] == 'update':
                event = update_event(change)
            elif change['kind'] == 'delete':
                event = delete_event(change)
            if event:
                event['sent_at'] = msg.send_time
                event['lsn'] = msg.data_start
                yield event

def process_event_stdout(event):
    print(json.dumps(event, sort_keys=True, default=str))

def consume_stream(msg, whitelist_regex_c, blacklist_regex_c, process_event_fn):
    logger.debug('got payload %s', msg.payload)
    for event in msg_to_events_generator(msg=msg):
        allowed = True
        if whitelist_regex_c:
            if not re.match(whitelist_regex_c, event['table']):
                logger.debug('did not pass whitelist %s', event['table'])
                allowed = False
        if allowed and blacklist_regex_c:
            if re.match(blacklist_regex_c, event['table']):
                logger.debug('matched blacklist %s', event['table'])
                allowed = False
        if allowed:
            process_event_fn(event=event)
        else:
            logger.debug('skipping event %s', event)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

def create_db_cursor(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot):
    conn = psycopg2.connect(f'host={pghost} port={pgport} dbname={pgdatabase} user={pguser} password={pgpassword}',
                                         connection_factory=LogicalReplicationConnection)
    cur = conn.cursor()
    logger.debug('trying to create replication slot %s', pgslot)
    try:
        cur.create_replication_slot(slot_name=pgslot, slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                            output_plugin='wal2json')
    except psycopg2.ProgrammingError as err:
        if err.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
            raise
        else:
            logger.debug('slot already exists, reusing')
    logger.debug('start replication')
    cur.start_replication(slot_name=pgslot, decode=True)
    logger.debug('started consuming')
    return cur


@click.command()
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pgport', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pgdatabase', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pguser', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pgpassword', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pgslot', default=lambda: os.environ.get('PGSLOT', None), required=True, help='Postgresql Replication Slot Name ($PGSLOT)')
@click.option('--whitelist-regex', required=False, help='Regex of schema.table to include - e.g. .*\.foo')
@click.option('--blacklist-regex', required=False, help='Regex of schema.table to exclude - e.g. testns\..*')
def stdout_writer(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, whitelist_regex, blacklist_regex):
    logging.basicConfig(level=logging.INFO)
    db_cur = create_db_cursor(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgslot=pgslot)
    whitelist_regex_c = re.compile(whitelist_regex) if whitelist_regex else None
    blacklist_regex_c = re.compile(blacklist_regex) if blacklist_regex else None
    db_cur.consume_stream(consume=lambda msg : consume_stream(msg=msg, whitelist_regex_c=whitelist_regex_c, blacklist_regex_c=blacklist_regex_c, process_event_fn=process_event_stdout))

def process_event_rabbitmq(rabbitmq_exchange, rabbitmq_channel, event):
    routing_key = f'{event["table"]}.{event["kind"]}'
    body = json.dumps(event, sort_keys=True, default=str)
    logger.debug('sending routing_key %s body %s ', routing_key, body)
    rabbitmq_channel.basic_publish(exchange=rabbitmq_exchange, routing_key=routing_key, body=body)

def create_rabbitmq_channel(rabbitmq_url, rabbitmq_exchange):
    conn = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    channel = conn.channel()
    channel.exchange_declare(exchange=rabbitmq_exchange, exchange_type='topic')
    return channel

@click.command()
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pgport', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pgdatabase', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pguser', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pgpassword', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pgslot', default=lambda: os.environ.get('PGSLOT', None), required=True, help='Postgresql Replication Slot Name ($PGSLOT)')
@click.option('--whitelist-regex', required=False, help='Regex of schema.table to include - e.g. .*\.foo')
@click.option('--blacklist-regex', required=False, help='Regex of schema.table to exclude - e.g. testns\..*')
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
def rabbitmq_writer(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, whitelist_regex, blacklist_regex, rabbitmq_url, rabbitmq_exchange):
    logging.basicConfig(level=logging.DEBUG)
    db_cur = create_db_cursor(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgslot=pgslot)
    whitelist_regex_c = re.compile(whitelist_regex) if whitelist_regex else None
    blacklist_regex_c = re.compile(blacklist_regex) if blacklist_regex else None
    rabbitmq_channel = create_rabbitmq_channel(rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    rabbitmq_sender_fn = lambda event: process_event_rabbitmq(rabbitmq_exchange=rabbitmq_exchange, rabbitmq_channel=rabbitmq_channel, event=event)
    db_cur.consume_stream(consume=lambda msg : consume_stream(msg=msg, whitelist_regex_c=whitelist_regex_c, blacklist_regex_c=blacklist_regex_c, process_event_fn=rabbitmq_sender_fn))


@click.command()
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
def rabbitmq_reader(rabbitmq_url, rabbitmq_exchange):
    logging.basicConfig(level=logging.DEBUG)
    rabbitmq_channel = create_rabbitmq_channel(rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    result = rabbitmq_channel.queue_declare('rabbitmq_reader', exclusive=False)
    queue_name = result.method.queue
    binding_key = '#'
    rabbitmq_channel.queue_bind(exchange=rabbitmq_exchange, queue=queue_name, routing_key=binding_key)
    def callback(ch, method, properties, body):
        print(" [x] %r:%r" % (method.routing_key, body))
    rabbitmq_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    rabbitmq_channel.start_consuming()
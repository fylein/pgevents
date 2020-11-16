import os
import json
import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection
import pika
import click
import logging

logger = logging.getLogger(__name__)

def consume_stream_stdout(msg):
    logger.info('got message %s', msg.payload)
    obj = json.loads(msg.payload)
    logger.info('parsed message %s', obj)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

def init_cursor(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot):
    conn = psycopg2.connect(f'host={pghost} port={pgport} dbname={pgdatabase} user={pguser} password={pgpassword}',
                                         connection_factory=LogicalReplicationConnection)
    cur = conn.cursor()
    logger.info('trying to create replication slot %s', pgslot)
    try:
        cur.create_replication_slot(slot_name=pgslot, slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                            output_plugin='wal2json')
    except psycopg2.ProgrammingError as err:
        if err.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
            raise
        else:
            logger.info('slot already exists, reusing')
    logger.info('start replication')
    cur.start_replication(slot_name=pgslot, decode=True)
    logger.info('started consuming')
    return cur

@click.command()
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pgport', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pgdatabase', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pguser', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pgpassword', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pgslot', default=lambda: os.environ.get('PGSLOT', None), required=True, help='Postgresql Replication Slot Name ($PGSLOT)')
def stdout(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot):
    logging.basicConfig(level=logging.DEBUG)
    cur = init_cursor(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgslot=pgslot)
    cur.consume_stream(consume_stream_stdout)

@click.command()
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
def rabbitmq(pghost):
    logging.basicConfig(level=logging.INFO)
    logger.info('not yet implemented')

# @cli.command('stdout')
# @click.pass_context
# def cli_stdout(ctx):
#     logger.info('you are in stdout with ctx %s', ctx)

# if __name__ == '__main__':
#     print(os.getenv('PGDATABASE'))
#     slot_name = 'pg_bifrost'

#     db_conn = psycopg2.connect('host=localhost dbname=test_db user=dc',
#                                          connection_factory=LogicalReplicationConnection)
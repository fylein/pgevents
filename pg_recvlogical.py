import os
import json
import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection
import pika
import click
import logging

logger = logging.getLogger(__name__)

@click.command()
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pgport', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pgdatabase', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pguser', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pgpassword', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
def stdout(pghost, pgport, pgdatabase, pguser, pgpassword):
    logging.basicConfig(level=logging.INFO)
    logger.info('pghost is %s', pghost)


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
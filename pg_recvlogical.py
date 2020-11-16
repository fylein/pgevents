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
def stdout(pghost):
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
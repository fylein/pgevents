import os
import json
import click
import logging
from libs.msg import consume_stream
from libs.pg import create_db_cursor
from libs.logging import init_logging

logger = logging.getLogger(__name__)

@click.command()
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pgport', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pgdatabase', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pguser', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pgpassword', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pgslot', default=lambda: os.environ.get('PGSLOT', None), required=True, help='Postgresql Replication Slot Name ($PGSLOT)')
@click.option('--pgtables', default=lambda: os.environ.get('PGTABLES', None), required=False, help='Restrict to specific tables e.g. public.transactions,public.reports')
def pg_to_stdout(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, pgtables):
    init_logging()
    db_cur = create_db_cursor(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgslot=pgslot, pgtables=pgtables)

    def print_event(event):
        print(json.dumps(event, sort_keys=True, default=str))

    db_cur.consume_stream(consume=lambda msg : consume_stream(msg=msg, process_event_fn=print_event))

if __name__ == '__main__':
    pg_to_stdout()

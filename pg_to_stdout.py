import os
import json
import click
import logging
import re
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
@click.option('--whitelist-regex', required=False, help='Regex of schema.table to include - e.g. .*\.foo')
@click.option('--blacklist-regex', required=False, help='Regex of schema.table to exclude - e.g. testns\..*')
@click.option('--log-level', default='ERROR', required=False, help='Print lots of debug logs (DEBUG, INFO, WARN, ERROR)')
def pg_to_stdout(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, whitelist_regex, blacklist_regex, log_level):
    init_logging(log_level)
    db_cur = create_db_cursor(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword, pgslot=pgslot)
    whitelist_regex_c = re.compile(whitelist_regex) if whitelist_regex else None
    blacklist_regex_c = re.compile(blacklist_regex) if blacklist_regex else None

    def print_event(event):
        print(json.dumps(event, sort_keys=True, default=str))

    db_cur.consume_stream(consume=lambda msg : consume_stream(msg=msg, whitelist_regex_c=whitelist_regex_c, blacklist_regex_c=blacklist_regex_c, process_event_fn=print_event))

if __name__ == '__main__':
    pg_to_stdout()

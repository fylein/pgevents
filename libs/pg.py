import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection
import logging

logger = logging.getLogger(__name__)

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
    cur.start_replication(slot_name=pgslot, options={'format-version': 2, 'include-types': False, 'add-tables': 'public.*'}, decode=True)
    logger.debug('started consuming')
    return cur

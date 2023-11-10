import json
import base64

from abc import ABC
from typing import Type, Union

import cachetools

import psycopg2
from psycopg2 import pool
from psycopg2.extras import LogicalReplicationConnection

from common.event import BaseEvent
from common.qconnector import QConnector

from common.log import get_logger

from common.utils import DeserializerUtils as parser_utils


logger = get_logger(__name__)

# 12 hour cache
# Maximum of 1024 entries
table_name_cache = cachetools.TTLCache(maxsize=1024, ttl=43200)
schema_cache = cachetools.TTLCache(maxsize=1024, ttl=43200)


class EventProducer(ABC):

    def __init__(self, *, qconnector_cls, event_cls, pg_host, pg_port, pg_database, pg_user, pg_password,
                 pg_tables, pg_replication_slot, pg_output_plugin, pg_publication_name=None, **kwargs):

        self.__shutdown = False
        self.event_cls = event_cls

        self.__db_conn: Union[psycopg2.connection, None] = None
        self.__db_cur: Union[psycopg2.cursor, None] = None
        self.__replication_cursor: Union[psycopg2.cursor, None] = None
        self.__db_conn_pool: Union[psycopg2.pool.SimpleConnectionPool, None] = None
        self.__pg_tables = pg_tables
        self.__pg_replication_slot = pg_replication_slot

        self.__pg_host = pg_host
        self.__pg_port = pg_port
        self.__pg_database = pg_database
        self.__pg_user = pg_user
        self.__pg_password = pg_password
        self.__pg_output_plugin = pg_output_plugin
        self.__pg_connection_factory = LogicalReplicationConnection

        self.__pg_publication_name = pg_publication_name

        self.qconnector_cls: Type[QConnector] = qconnector_cls
        self.qconnector: QConnector = qconnector_cls(**kwargs)

    def __connect_db(self):
        self.__db_conn_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=2,
            host=self.__pg_host,
            port=self.__pg_port,
            dbname=self.__pg_database,
            user=self.__pg_user,
            password=self.__pg_password,
            connection_factory=self.__pg_connection_factory
        )
        self.__db_cur = self.__db_conn_pool.getconn().cursor()

        self.__replication_cursor = self.__db_conn_pool.getconn().cursor()

        if self.__pg_output_plugin == 'wal2json':
            decode = True

            options = {
                'format-version': 2,
                'include-types': True,
                'include-lsn': True
            }
            if self.__pg_tables and len(self.__pg_tables) > 0:
                options['add-tables'] = self.__pg_tables
        else:
            options = {
                'proto_version': 1,
                'publication_names': self.__pg_publication_name
            }

            decode = False

        logger.info('Creating Replication Slot if not exists...')
        self.__create_replication_slot()

        logger.debug('options for slot %s', options)
        self.__replication_cursor.start_replication(
            slot_name=self.__pg_replication_slot,
            options=options,
            decode=decode
        )

    def connect(self):
        self.qconnector.connect()

        logger.info('Connecting to postgres...')
        self.__connect_db()

    def __create_replication_slot(self) -> None:
        """
        Create a new logical replication slot.

        Args:
            slot_name (str): The name of the replication slot to create.
        """
        try:
            cursor = self.__db_cur
            cursor.execute(
                "select pg_create_logical_replication_slot(%s, %s);",
                (self.__pg_replication_slot, self.__pg_output_plugin)
            )
            logger.debug('Replication slot created')
        except psycopg2.errors.DuplicateObject:
            logger.debug('Replication slot already exists')
        except psycopg2.errors.OperationalError:
            logger.exception("Operational error during initialization.")
            raise psycopg2.errors.OperationalError("Operational error during initialization.")

    def wal2json_msg_processor(self, msg):
        pl = json.loads(msg.payload)

        if pl['action'] in ['I', 'U', 'D']:
            table_name = f"{pl['schema']}.{pl['table']}"

            event: BaseEvent = self.event_cls()
            event.load_wal2json_payload(msg.payload)

            modified_event: BaseEvent
            event_routing_key, modified_event = self.get_event_routing_key_and_event(table_name, event)

            # If no routing key is provided, then the event will not be queued
            if event_routing_key is not None:
                payload = modified_event.to_dict()
                json_payload = json.dumps(payload)
                self.publish(
                    routing_key=event_routing_key,
                    payload=json_payload
                )

        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        self.check_shutdown()

    @cachetools.cached(cache=table_name_cache, key=lambda self, relation_id: cachetools.keys.hashkey(relation_id))
    def __get_table_name(self, relation_id: int) -> str:
        """
        Get the table name using the relation ID.

        Args:
            relation_id (int): The relation ID of the table.

        Returns:
            str: Full table name including schema.
        """
        try:
            logger.debug('Getting table name for relation ID: %s', relation_id)
            self.__db_cur.execute(
                "select schemaname, relname from pg_stat_user_tables where relid = %s;",
                (relation_id,)
            )
            schema_name, table_name = self.__db_cur.fetchone()
            return f'{schema_name}.{table_name}'
        except Exception:
            logger.exception("Failed to get table name.")
            raise
    
    @cachetools.cached(cache=schema_cache, key=lambda self, relation_id: cachetools.keys.hashkey(relation_id))
    def __get_schema(self, relation_id) -> dict:
        """
        Retrieve the schema for the relation.
        :return: A dictionary containing the schema information.
        """
        logger.debug(f'Relation ID: {relation_id}')
        logger.debug('Getting Schema...')

        schema = {
            'relation_id': relation_id,
            'columns': []
        }

        logger.debug('Getting column names and types...')
        self.__db_cur.execute(
            f'SELECT attname, atttypid FROM pg_attribute WHERE attrelid = {relation_id} AND attnum > 0;'
        )

        for column in self.__db_cur.fetchall():
            schema['columns'].append({
                'name': column[0],
                'type': column[1]
            })

        logger.debug(f'Schema retrieved successfully. {schema}')
        return schema

    def pgoutput_msg_processor(self, msg):
        message_type = msg.payload[:1].decode('utf-8')

        # Convert bytes to int
        relation_id = parser_utils.convert_bytes_to_int(msg.payload[1:5])

        if message_type in ['I', 'U', 'D']:
            operation_type = 'INSERT' if message_type == 'I' else 'UPDATE' if message_type == 'U' else 'DELETE'
            table_name = self.__get_table_name(relation_id)

            if table_name in self.__pg_tables:
                logger.debug(f'{operation_type} Change occurred on table: {table_name}')
                logger.debug(f'{operation_type} Change occurred at LSN: {msg.data_start}')

                payload = {
                    'schema': self.__get_schema(relation_id),
                    'payload': base64.b64encode(msg.payload).decode('utf-8')
                }
                logger.debug(f'{operation_type} Change payload: {payload}')

                self.publish(
                    routing_key=table_name,
                    payload=json.dumps(payload)
                )

                logger.debug(f'{operation_type} Change processed on table: {table_name}')
                logger.debug(f'{operation_type} Change processed at LSN: {msg.data_start}')

        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        self.check_shutdown()

    def start_consuming(self):
        def stream_consumer(msg):
            logger.info('Received message: %s', msg)
            if self.__pg_output_plugin == 'wal2json':
                self.wal2json_msg_processor(msg=msg)
            else:
                self.pgoutput_msg_processor(msg=msg)
            self.check_shutdown()

        self.__replication_cursor.consume_stream(consume=stream_consumer)

    def get_event_routing_key_and_event(self, table_name: str, event: BaseEvent) -> (str, BaseEvent):
        return table_name, event

    def publish(self, **kwargs):
        self.qconnector.publish(**kwargs)

    def shutdown(self):
        logger.warning('Shutdown triggered')
        self.__shutdown = True
        self.qconnector.shutdown()

    def check_shutdown(self):
        self.qconnector.check_shutdown()

        if self.__shutdown and self.__db_conn:
            logger.warning('Shutting down...')
            self.__db_conn.close()

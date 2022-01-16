import json
from abc import ABC, abstractmethod
from typing import Union

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from fyle_pgevents.event import BaseEvent
from fyle_pgevents.qconnector import QConnector

from fyle_pgevents.common.log import get_logger

logger = get_logger(__name__)


class PGEventProducer(ABC):

    def __init__(self, *, qconnector_cls, event_cls, pg_host, pg_port, pg_database, pg_user, pg_password,
                 pg_tables, pg_replication_slot, **kwargs):

        self.__shutdown = False
        self.event_cls = event_cls

        self.__db_conn: Union[psycopg2.connection, None] = None
        self.__db_cur: Union[psycopg2.cursor, None] = None

        self.__pg_tables = pg_tables
        self.__pg_replication_slot = pg_replication_slot

        self.__pg_host = pg_host
        self.__pg_port = pg_port
        self.__pg_database = pg_database
        self.__pg_user = pg_user
        self.__pg_password = pg_password
        self.__pg_connection_factory = LogicalReplicationConnection

        self.qconnector_cls: Type[QConnector] = qconnector_cls
        self.qconnector: QConnector = qconnector_cls(**kwargs)

    def __connect_db(self):
        self.__db_conn = psycopg2.connect(
            host=self.__pg_host,
            port=self.__pg_port,
            dbname=self.__pg_database,
            user=self.__pg_user,
            password=self.__pg_password,
            connection_factory=self.__pg_connection_factory
        )
        self.__db_cur = self.__db_conn.cursor()

        options = {
            'format-version': 2,
            'include-types': True,
            'include-lsn': True
        }

        if self.__pg_tables and len(self.__pg_tables) > 0:
            options['add-tables'] = self.__pg_tables

        logger.debug('options for slot %s', options)
        self.__db_cur.start_replication(
            slot_name=self.__pg_replication_slot,
            options=options,
            decode=True
        )

    def connect(self):
        self.qconnector.connect()

        logger.info('Connecting to postgres...')
        self.__connect_db()

    def msg_processor(self, msg):
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

    def start_consuming(self):
        def stream_consumer(msg):
            self.msg_processor(msg=msg)
            self.check_shutdown()

        self.__db_cur.consume_stream(consume=stream_consumer)

    @abstractmethod
    def get_event_routing_key_and_event(self, table_name: str, event: BaseEvent) -> (str, BaseEvent):
        pass

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

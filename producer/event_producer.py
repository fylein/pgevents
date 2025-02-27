import json
import threading

from abc import ABC
from typing import Type, Union, List

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from common.event import BaseEvent
from common.qconnector import QConnector

from common.log import get_logger

from common.utils import DeserializerUtils as parser_utils
from pgoutput_parser import (
    UpdateMessage,
    InsertMessage,
    DeleteMessage,
    RelationMessage
)


logger = get_logger(__name__)


class EventProducer(ABC):

    def __init__(self, *, qconnector_cls, event_cls, pg_host, pg_port, pg_database, pg_user, pg_password,
                 pg_tables, pg_replication_slot, pg_output_plugin, pg_publication_name=None, **kwargs):

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
        self.__pg_output_plugin = pg_output_plugin
        self.__pg_connection_factory = LogicalReplicationConnection

        self.__pg_publication_name = pg_publication_name

        self.__table_schemas = {}

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

        logger.info('Creating Replication Slot if not exists...')
        self.__create_replication_slot()

        logger.debug('options for slot %s', options)
        self.__db_cur.start_replication(
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


    def __drop_replication_slot(self):
        conn = psycopg2.connect(
            host=self.__pg_host,
            port=self.__pg_port,
            dbname=self.__pg_database,
            user=self.__pg_user,
            password=self.__pg_password
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_drop_replication_slot(%s);", (self.__pg_replication_slot,))
                conn.commit()
                print(f"Replication slot {self.__pg_replication_slot} dropped successfully.")
        except Exception as e:
            print(f"Error dropping replication slot {self.__pg_replication_slot}: {e}")
        finally:
            conn.close()


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

    def pgoutput_msg_processor(self, msg):
        message_type = msg.payload[:1].decode('utf-8')

        if message_type == 'R':
            logger.debug(f'Received R message with lsn: {msg.data_start}')

            parser = RelationMessage(table_name=None, message=msg.payload, schema=None)
            parsed_message = parser.decode_relation_message()
            self.__table_schemas[parsed_message['relation_id']] = parsed_message

        if message_type in ['I', 'U', 'D']:
            relation_id = parser_utils.convert_bytes_to_int(msg.payload[1:5])

            schema = self.__table_schemas[relation_id]
            table_name = self.__table_schemas[relation_id]['table_name']

            logger.debug(f'Table name: {table_name}')
            logger.debug(f'Schema: {schema}')

            if table_name in self.__pg_tables or '.*' in self.__pg_tables:
                logger.debug(f'Received {message_type} message with lsn: {msg.data_start} for table: {table_name}')
                
                parsed_message = None
                if message_type == 'I':
                    logger.debug(f'INSERT Message, Message Type: {message_type} - {table_name}')
                    parser = InsertMessage(table_name=table_name, message=msg.payload, schema=schema)
                    parsed_message = parser.decode_insert_message()

                elif message_type == 'U':
                    logger.debug(f'UPDATE Message, Message Type: {message_type} - {table_name}')
                    parser = UpdateMessage(table_name=table_name, message=msg.payload, schema=schema)
                    parsed_message = parser.decode_update_message()

                elif message_type == 'D':
                    logger.debug(f'DELETE Message, Message Type: {message_type} - {table_name}')
                    parser = DeleteMessage(table_name=table_name, message=msg.payload, schema=schema)
                    parsed_message = parser.decode_delete_message()

                if parsed_message:
                    routing_key = f"{self.__pg_database}.{table_name}"
                    self.publish(
                        routing_key=routing_key,
                        payload=json.dumps(parsed_message)
                    )
                    logger.debug(f'Published message to queue: {parsed_message}')
                else:
                    logger.warning(f'Skipping {message_type} message for table {table_name} - invalid or empty message')

                logger.debug(f'Ack: Message {message_type} with lsn: {msg.data_start} for table: {table_name}')

        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        self.check_shutdown()

    def start_consuming(self):
        def stream_consumer(msg):
            try:
                if self.__shutdown:
                    logger.info('Shutdown requested, stopping consumer')
                    return

                logger.info('Received message: %s', msg)
                if self.__pg_output_plugin == 'wal2json':
                    self.wal2json_msg_processor(msg=msg)
                else:
                    self.pgoutput_msg_processor(msg=msg)
            
            except Exception as e:
                logger.error(f'Error processing message: {e}')
                if self.__shutdown:
                    raise
            finally:
                self.check_shutdown()

        try:
            self.__db_cur.consume_stream(consume=stream_consumer)
        except Exception as e:
            if self.__shutdown:
                logger.info('Shutting down gracefully')
                return
            raise

    def get_event_routing_key_and_event(self, table_name: str, event: BaseEvent) -> (str, BaseEvent):
        return table_name, event

    def publish(self, **kwargs):
        self.qconnector.publish(**kwargs)

    def shutdown(self):
        """Gracefully shutdown the producer"""
        logger.warning('Shutdown triggered')
        self.__shutdown = True
        
        if self.__db_conn and not self.__db_conn.closed:
            try:
                self.__db_cur.close()
                self.__db_conn.close()
                logger.info('Database connection closed successfully')
            except Exception as e:
                logger.error(f'Error closing database connection: {e}')

        try:
            self.__drop_replication_slot()
        except Exception as e:
            logger.error(f'Error dropping replication slot: {e}')

        try:
            self.qconnector.shutdown()
        except Exception as e:
            logger.error(f'Error shutting down queue connection: {e}')

    def check_shutdown(self):
        """Check if shutdown was requested and raise SystemExit if true"""
        self.qconnector.check_shutdown()
        if self.__shutdown:
            logger.warning('Shutdown requested, raising SystemExit')
            raise SystemExit('Shutdown requested')

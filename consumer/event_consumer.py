import json
from abc import ABC
from typing import Union, Type

import psycopg2

from common.event import BaseEvent
from common.qconnector import QConnector
from common import log
from pgoutput_parser.delete import DeleteMessage
from pgoutput_parser.insert import InsertMessage
from pgoutput_parser.update import UpdateMessage


logger = log.get_logger(__name__)


class EventConsumer(ABC):

    def __init__(
            self, *, pg_host, pg_port, pg_database, 
            pg_user, pg_password, qconnector_cls, pg_output_plugin, event_cls, **kwargs
        ):
        self.__shutdown = False
        self.event_cls = event_cls

        self.__pg_host = pg_host
        self.__pg_port = pg_port
        self.__pg_database = pg_database
        self.__pg_user = pg_user
        self.__pg_password = pg_password

        self.__pg_output_plugin = pg_output_plugin
        self.__dbconn: Union[psycopg2.connection, None] = None

        kwargs['use_compression'] = True if self.__pg_output_plugin == 'wal2json' else False

        self.qconnector_cls: Type[QConnector] = qconnector_cls
        self.qconnector: QConnector = qconnector_cls(**kwargs)

    def process_message(self, routing_key, event: BaseEvent):
        logger.info('routing_key %s' % routing_key)
        logger.info('event %s' % event)
        logger.info('event %s' % event.to_dict())

    def connect(self):
        if self.__pg_output_plugin == 'pgoutput':
            self.__dbconn = psycopg2.connect(
                host=self.__pg_host,
                port=self.__pg_port,
                dbname=self.__pg_database,
                user=self.__pg_user,
                password=self.__pg_password
            )
        self.qconnector.connect()
    
    def __parse_pgoutput_message(self, table_name, data):
        logger.debug(f'Incoming message: {data}')
        message_type = data[:1].decode('utf-8')
        parsed_message = {}
        cursor = self.__dbconn.cursor()

        if message_type == 'I':
            logger.info(f'INSERT Message, Message Type: {message_type} - {table_name}')
            parser = InsertMessage(table_name=table_name, message=data, cursor=cursor)
            parsed_message = parser.decode_insert_message()

        elif message_type == 'U':
            logger.info(f'UPDATE Message, Message Type: {message_type} - {table_name}')
            parser = UpdateMessage(table_name=table_name, message=data, cursor=cursor)
            parsed_message = parser.decode_update_message()

        elif message_type == 'D':
            logger.info(f'DELETE Message, Message Type: {message_type} - {table_name}')
            parser = DeleteMessage(table_name=table_name, message=data, cursor=cursor)
            parsed_message = parser.decode_delete_message()

        cursor.close()

        return parsed_message
 

    def start_consuming(self):
        def stream_consumer(routing_key, payload):
            if self.__pg_output_plugin == 'wal2json':
                payload_dict = json.loads(payload)
            else:
                payload_dict = self.__parse_pgoutput_message(routing_key, payload)

            event: BaseEvent = self.event_cls()
            event.from_dict(payload_dict)

            self.process_message(routing_key, event)
            self.check_shutdown()

        self.qconnector.consume_stream(
            callback_fn=stream_consumer
        )

    def shutdown(self):
        self.__shutdown = True
        self.qconnector.shutdown()

    def check_shutdown(self):
        self.qconnector.check_shutdown()

        if self.__shutdown:
            pass

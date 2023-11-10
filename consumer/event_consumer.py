import json
import base64
from abc import ABC
from typing import Type

from common.event import BaseEvent
from common.qconnector import QConnector
from common import log
from pgoutput_parser.delete import DeleteMessage
from pgoutput_parser.insert import InsertMessage
from pgoutput_parser.update import UpdateMessage


logger = log.get_logger(__name__)


class EventConsumer(ABC):

    def __init__(
            self, *, qconnector_cls, pg_output_plugin, event_cls, **kwargs
        ):
        self.__shutdown = False
        self.event_cls = event_cls

        self.__pg_output_plugin = pg_output_plugin

        self.qconnector_cls: Type[QConnector] = qconnector_cls
        self.qconnector: QConnector = qconnector_cls(**kwargs)

    def process_message(self, routing_key, event: BaseEvent):
        logger.info('routing_key %s' % routing_key)
        logger.info('event %s' % event)
        logger.info('event %s' % event.to_dict())

    def connect(self):
        self.qconnector.connect()
    
    def __parse_pgoutput_message(self, table_name, data):
        logger.debug(f'Incoming message: {data}')
        schema = data['schema']
        message = base64.b64decode(data['payload'])

        message_type = message[:1].decode('utf-8')
        parsed_message = {}

        if message_type == 'I':
            logger.info(f'INSERT Message, Message Type: {message_type} - {table_name}')
            parser = InsertMessage(table_name=table_name, message=message, schema=schema)
            parsed_message = parser.decode_insert_message()

        elif message_type == 'U':
            logger.info(f'UPDATE Message, Message Type: {message_type} - {table_name}')
            parser = UpdateMessage(table_name=table_name, message=message, schema=schema)
            parsed_message = parser.decode_update_message()

        elif message_type == 'D':
            logger.info(f'DELETE Message, Message Type: {message_type} - {table_name}')
            parser = DeleteMessage(table_name=table_name, message=message, schema=schema)
            parsed_message = parser.decode_delete_message()

        return parsed_message
 

    def start_consuming(self):
        def stream_consumer(routing_key, payload):
            payload_dict = json.loads(payload)

            if self.__pg_output_plugin == 'pgoutput':
                payload_dict = self.__parse_pgoutput_message(routing_key, payload_dict)

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

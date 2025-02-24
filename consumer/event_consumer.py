import json
from abc import ABC, abstractmethod
from typing import Type

from common.event import BaseEvent
from common.qconnector import QConnector
from common import log


logger = log.get_logger(__name__)


class EventConsumer(ABC):

    def __init__(self, *, qconnector_cls, event_cls, **kwargs):
        self.__shutdown = False
        self.event_cls = event_cls

        self.qconnector_cls: Type[QConnector] = qconnector_cls
        self.qconnector: QConnector = qconnector_cls(**kwargs)

    def process_message(self, routing_key, event: BaseEvent, delivery_tag: int):
        logger.info('routing_key %s' % routing_key)
        logger.info('event %s' % event)
        logger.info('event %s' % event.to_dict())

    def connect(self):
        self.qconnector.connect()

    def start_consuming(self):
        def stream_consumer(routing_key, payload, properties=None, delivery_tag=None):
            payload_dict = json.loads(payload)

            if self.event_cls:
                event: BaseEvent = self.event_cls()
                event.from_dict(payload_dict)
            else:
                event = payload_dict

            self.process_message(routing_key, event, delivery_tag)

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

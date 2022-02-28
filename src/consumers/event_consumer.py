import json
from abc import ABC, abstractmethod

from src.event import BaseEvent
from src.qconnector import QConnector


class EventConsumer(ABC):

    def __init__(self, *, qconnector_cls, event_cls, **kwargs):
        self.__shutdown = False
        self.event_cls = event_cls

        self.qconnector_cls: Type[QConnector] = qconnector_cls
        self.qconnector: QConnector = qconnector_cls(**kwargs)

    @abstractmethod
    def process_message(self, routing_key, event):
        pass

    def connect(self):
        self.qconnector.connect()

    def start_consuming(self):
        def stream_consumer(routing_key, payload):
            payload_dict = json.loads(payload)

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

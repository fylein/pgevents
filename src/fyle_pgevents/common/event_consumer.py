from abc import ABC, abstractmethod
from fyle_pgevents.common.q_connector import QConnector


class EventConsumer(ABC):

    def __init__(self, *, qconnector_cls, **kwargs):
        self.__shutdown = False

        self.qconnector_cls: Type[QConnector] = qconnector_cls
        self.qconnector: QConnector = qconnector_cls(**kwargs)

    @abstractmethod
    def process_message(self, routing_key, payload):
        pass

    def connect(self):
        self.qconnector.connect()

    def start_consuming(self):
        def stream_consumer(routing_key, payload):
            self.process_message(routing_key, payload)
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

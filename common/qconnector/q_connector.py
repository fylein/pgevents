from abc import ABC, abstractmethod

from common.log import get_logger

logger = get_logger(__name__)


class QConnector(ABC):
    def __init__(self):
        self.__shutdown = False

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def publish(self, *, routing_key, body):
        pass

    @abstractmethod
    def consume_stream(self, callback_fn):
        pass

    @abstractmethod
    def consume_all(self):
        pass

    def shutdown(self):
        logger.warning('Shutdown has been requested')
        self.__shutdown = True

    def check_shutdown(self):
        if self.__shutdown:
            logger.warning('Shutting down now...')
            self.disconnect()

    @abstractmethod
    def acknowledge_message(self, delivery_tag):
        pass

    @abstractmethod
    def reject_message(self, delivery_tag, requeue=False):
        pass
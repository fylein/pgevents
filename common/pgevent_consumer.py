import json
import logging

import pika

from common.decorators import retry
from common.compression import decompress

logger = logging.getLogger(__name__)

class PGEventConsumerShutdownException(Exception):
    pass

class PGEventConsumer:
    def __init__(self, rabbitmq_url, rabbitmq_exchange, queue_name, binding_keys, process_event_fn):
        self.__rabbitmq_url = rabbitmq_url
        self.__rabbitmq_exchange = rabbitmq_exchange
        self.__queue_name = queue_name
        self.__binding_keys = binding_keys
        self.__rmq_conn = None
        self.__rmq_channel = None
        self.__shutdown = False
        self.__process_event_fn = process_event_fn
        self.__connect_rabbitmq()

    @retry(n=3, backoff=15, exceptions=(pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError))
    def __connect_rabbitmq(self):
        self.__check_shutdown()
        self.__rmq_conn = pika.BlockingConnection(pika.URLParameters(self.__rabbitmq_url))
        self.__rmq_channel = self.__rmq_conn.channel()
        self.__rmq_channel.exchange_declare(exchange=self.__rabbitmq_exchange, exchange_type='topic', durable=True)
        res = self.__rmq_channel.queue_declare(self.__queue_name, durable=True, exclusive=False, auto_delete=True)

        self.__queue_name = res.method.queue
        for binding_key in self.__binding_keys.split(','):
            logger.info('binding to exchange %s, queue %s, binding_key %s', self.__rabbitmq_exchange, self.__queue_name, binding_key)
            self.__rmq_channel.queue_bind(exchange=self.__rabbitmq_exchange, queue=self.__queue_name, routing_key=binding_key)

    def __check_shutdown(self):
        if self.__shutdown:
            raise PGEventConsumerShutdownException('shutting down')

    def __process_body(self, ch, method, properties, body):
        #pylint: disable=unused-argument
        self.__check_shutdown()
        bodyu = decompress(body)
        event = json.loads(bodyu)
        self.__process_event_fn(event)

    def process(self):
        try:
            self.__rmq_channel.basic_consume(queue=self.__queue_name, on_message_callback=self.__process_body, auto_ack=True)
            self.__rmq_channel.start_consuming()
        except PGEventConsumerShutdownException:
            logger.warning('exiting process loop')
            return

    def shutdown(self, *args):
        #pylint: disable=unused-argument
        logger.warning('Shutdown has been requested')
        self.__shutdown = True

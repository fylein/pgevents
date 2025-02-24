import pika
from common.qconnector import QConnector

from common.compression import compress, decompress
from common.log import get_logger

logger = get_logger(__name__)


class RabbitMQConnector(QConnector):
    def __init__(self, rabbitmq_url, rabbitmq_exchange, queue_name=None, binding_keys=None):
        self.__rabbitmq_url = rabbitmq_url
        self.__rabbitmq_exchange = rabbitmq_exchange

        self.__rmq_conn = None
        self.__rmq_channel = None
        self.__queue_name = queue_name
        self.__binding_keys = binding_keys

        super().__init__()

    def disconnect(self):
        if self.__rmq_conn:
            self.__rmq_conn.close()

    def publish(self, routing_key, payload):
        compressed_body = compress(payload)
        logger.info('sending message with routing_key %s compressed_body bytes %s ', routing_key, len(compressed_body))
        self.__rmq_channel.basic_publish(
            exchange=self.__rabbitmq_exchange,
            routing_key=routing_key,
            body=compressed_body,
            properties=pika.BasicProperties(delivery_mode=2)  # persistent delivery mode
        )

    def consume_stream(self, callback_fn):
        def stream_consumer(ch, method, properties, body):
            callback_fn(
                routing_key=method.routing_key,
                payload=decompress(body)
            )
            self.check_shutdown()

        self.__rmq_channel.basic_consume(
            queue=self.__queue_name,
            on_message_callback=stream_consumer,
            auto_ack=True
        )
        self.__rmq_channel.start_consuming()

    def consume_all(self):
        routing_key_events = []

        while True:
            method, _, body = self.__rmq_channel.basic_get(
                self.__queue_name, True
            )
            if method is None:
                break

            routing_key_events.append(
                (method.routing_key, decompress(body))
            )

        return routing_key_events

    def connect(self):
        self.__rmq_conn = pika.BlockingConnection(
            parameters=pika.URLParameters(self.__rabbitmq_url)
        )
        self.__rmq_channel = self.__rmq_conn.channel()
        self.__rmq_channel.exchange_declare(
            exchange=self.__rabbitmq_exchange,
            exchange_type='topic',
            durable=True
        )

        # if messages are to be consumed from the connection
        if self.__queue_name and self.__binding_keys:
            res = self.__rmq_channel.queue_declare(
                queue=self.__queue_name,
                durable=True,  # Survive reboots of the broker
                exclusive=False,  # Only allow access by the current connection
                auto_delete=False  # Delete after consumer cancels or disconnects
            )

            self.__queue_name = res.method.queue
            logger.info('queue declared: %s', self.__queue_name)

            for binding_key in self.__binding_keys.split(','):
                logger.info('binding to exchange %s with queue %s and binding_key %s',
                            self.__rabbitmq_exchange, self.__queue_name, binding_key)

                self.__rmq_channel.queue_bind(
                    exchange=self.__rabbitmq_exchange,
                    queue=self.__queue_name,
                    routing_key=binding_key
                )

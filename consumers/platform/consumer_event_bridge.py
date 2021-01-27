import logging
import os
import signal
import json
import click
import pika

from common.decorators import retry
from common.logging import init_logging
from common.pgevent_consumer import PGEventConsumer
from .pg_platform_event import PGPlatformEvent

logger = logging.getLogger(__name__)


class ConsumerEventBridge:
    def __init__(self, rabbitmq_url, rabbitmq_exchange):
        self.__rabbitmq_url = rabbitmq_url
        self.__rabbitmq_exchange = rabbitmq_exchange
        self.__rmq_conn = None
        self.__rmq_channel = None
        self.__connect_rabbitmq()

    @retry(n=3, backoff=15, exceptions=(pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError))
    def __connect_rabbitmq(self):
        self.__rmq_conn = pika.BlockingConnection(pika.URLParameters(self.__rabbitmq_url))
        self.__rmq_channel = self.__rmq_conn.channel()
        self.__rmq_channel.exchange_declare(exchange=self.__rabbitmq_exchange, exchange_type='topic', durable=True)

    def publish(self, routing_key, body):
        logger.debug('publishing routing_key: %s body: %s', routing_key, body)
        self.__rmq_channel.basic_publish(
            exchange=self.__rabbitmq_exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def __call__(self, event):
        logger.debug('got event %s: %s', event.tablename, event.action)

        routing_key = None
        payload = None

        logger.debug('event.updated_by: %s', json.dumps(event.updated_by))
        if event.updated_by is None or event.updated_by.get('written_from') != 'platform-api':
            # raise event only when updated_by exists and value of written_from is 'platform-api'
            return

        # to move this to a separate transform function
        if event.tablename == 'platform_schema.employees_rot':
            if event.action == 'I':
                routing_key = 'eous.created'
                payload = {
                    'id': event.id,
                    'body': {
                        'inviterId': event.updated_by['employee_id']
                    }
                }
            elif event.action == 'U':
                routing_key = 'eous.policy_run'
                payload = {
                    'id': event.id
                }

        if payload is None:
            payload = {}

        body = json.dumps(payload)

        if routing_key is not None:
            self.publish(routing_key, body)


@click.command()
@click.option('--public-rabbitmq-url', default=lambda: os.environ.get('PUBLIC_RABBITMQ_URL', None), required=True, help='Public RabbitMQ url ($PUBLIC_RABBITMQ_URL)')
@click.option('--public-rabbitmq-exchange', default=lambda: os.environ.get('PUBLIC_RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ url ($PUBLIC_RABBITMQ_EXCHANGE)')
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
@click.option('--binding-keys', default=lambda: os.environ.get('RABBITMQ_BINDING_KEYS', '#'), required=True, help='RabbitMQ binding keys ($RABBITMQ_BINDING_KEYS, "#")')
@click.option('--queue-name', default=lambda: os.environ.get('RABBITMQ_QUEUE_NAME', ''), required=True, help='RabbitMQ queue name ($RABBITMQ_QUEUE_NAME, "")')
def consumer_event_bridge(public_rabbitmq_url, public_rabbitmq_exchange, rabbitmq_url, rabbitmq_exchange, binding_keys, queue_name):
    init_logging()

    process_event_fn = ConsumerEventBridge(
        rabbitmq_url=public_rabbitmq_url,
        rabbitmq_exchange=public_rabbitmq_exchange
    )

    pgevent_consumer = PGEventConsumer(
        rabbitmq_url=rabbitmq_url,
        rabbitmq_exchange=rabbitmq_exchange,
        queue_name=queue_name,
        binding_keys=binding_keys,
        process_event_fn=process_event_fn,
        pg_msg_event_cls=PGPlatformEvent
    )

    signal.signal(signal.SIGTERM, pgevent_consumer.shutdown)
    signal.signal(signal.SIGINT, pgevent_consumer.shutdown)
    pgevent_consumer.process()


if __name__ == '__main__':
    consumer_event_bridge()

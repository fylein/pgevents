import os
import click
import logging
from libs.rabbitmq import create_rabbitmq_channel

logger = logging.getLogger(__name__)

@click.command()
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
@click.option('--binding-keys', default=lambda: os.environ.get('RABBITMQ_BINDING_KEYS', '#'), required=True, help='RabbitMQ binding keys ($RABBITMQ_BINDING_KEYS, "#")')
@click.option('--queue-name', default=lambda: os.environ.get('RABBITMQ_QUEUE_NAME', ''), required=True, help='RabbitMQ queue name ($RABBITMQ_QUEUE_NAME, "")')
def rabbitmq_to_stdout(rabbitmq_url, rabbitmq_exchange, binding_keys, queue_name):
    logging.basicConfig(level=logging.INFO)
    rabbitmq_channel = create_rabbitmq_channel(rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    result = rabbitmq_channel.queue_declare(queue_name, durable=True, exclusive=False, auto_delete=False)
    queue_name = result.method.queue
    for binding_key in binding_keys.split(','):
        logging.info('binding to exchange %s, queue %s, binding_key %s', rabbitmq_exchange, queue_name, binding_key)
        rabbitmq_channel.queue_bind(exchange=rabbitmq_exchange, queue=queue_name, routing_key=binding_key)
    def callback(ch, method, properties, body):
        logger.info("[received] %r:%r" % (method.routing_key, body))
        print('[received] {method.routing_key} {body}')
    rabbitmq_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    rabbitmq_channel.start_consuming()

if __name__ == '__main__':
    rabbitmq_to_stdout()

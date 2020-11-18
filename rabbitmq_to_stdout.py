import os
import click
import logging
import json
from libs.rabbitmq import create_rabbitmq_channel
from libs.logging import init_logging

logger = logging.getLogger(__name__)

def clean_dict(d):
    for k in d:
        v = d[k]
        if v is None:
            del d[k]
    return d

def clean_event(event):
    del event['types']
    if event['kind'] == 'update':
        event['old'] = None
        event['new'] = None
    if event['old']:
        event['old'] = clean_dict(event['old'])
    if event['new']:
        event['new'] = clean_dict(event['new'])
    if event['diff']:
        event['diff'] = clean_dict(event['diff'])
    return event

@click.command()
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
@click.option('--binding-keys', default=lambda: os.environ.get('RABBITMQ_BINDING_KEYS', '#'), required=True, help='RabbitMQ binding keys ($RABBITMQ_BINDING_KEYS, "#")')
@click.option('--queue-name', default=lambda: os.environ.get('RABBITMQ_QUEUE_NAME', ''), required=True, help='RabbitMQ queue name ($RABBITMQ_QUEUE_NAME, "")')
def rabbitmq_to_stdout(rabbitmq_url, rabbitmq_exchange, binding_keys, queue_name):
    init_logging()
    logger.info('trying to open rabbitmq channel')
    rabbitmq_channel = create_rabbitmq_channel(rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    result = rabbitmq_channel.queue_declare(queue_name, durable=True, exclusive=True, auto_delete=True)
    queue_name = result.method.queue
    for binding_key in binding_keys.split(','):
        logger.info('binding to exchange %s, queue %s, binding_key %s', rabbitmq_exchange, queue_name, binding_key)
        rabbitmq_channel.queue_bind(exchange=rabbitmq_exchange, queue=queue_name, routing_key=binding_key)

    def callback(ch, method, properties, body):
        event = json.loads(body)
        event = clean_event(event)
        logger.info("got event %s", event)
        print(f'[received] {method.routing_key} {event}')

    rabbitmq_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    rabbitmq_channel.start_consuming()

if __name__ == '__main__':
    rabbitmq_to_stdout()

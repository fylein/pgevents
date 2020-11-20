import pika
import logging

logger = logging.getLogger(__name__)

def create_rabbitmq_channel(rabbitmq_url, rabbitmq_exchange):
    conn = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    channel = conn.channel()
    channel.exchange_declare(exchange=rabbitmq_exchange, exchange_type='topic')
    return channel


from common.event import base_event
from common.qconnector.rabbitmq_connector import RabbitMQConnector
from producer.event_producer import EventProducer


# Path: tests/test_event_producer.py


def test_producer_init(producer_init_params):
    p = EventProducer(**producer_init_params)

    assert p.qconnector_cls == RabbitMQConnector
    assert p.event_cls == base_event.BaseEvent
    assert p._EventProducer__pg_host == 'localhost'
    assert p._EventProducer__pg_port == 5432
    assert p._EventProducer__pg_database == 'test'
    assert p._EventProducer__pg_user == 'test'
    assert p._EventProducer__pg_password == 'test'
    assert p._EventProducer__pg_replication_slot == 'test'
    assert p._EventProducer__pg_output_plugin == 'pgoutput'
    assert p._EventProducer__pg_publication_name == 'test'
    assert p.qconnector._RabbitMQConnector__rabbitmq_url == 'amqp://admin:password@rabbitmq:5672/?heartbeat=0'
    assert p.qconnector._RabbitMQConnector__rabbitmq_exchange == 'test'


def test_producer_connect(mock_producer, mock_pika_connect, mock_pg_conn):
    mock_producer.connect()

    assert mock_producer._EventProducer__db_conn is not None
    assert mock_producer._EventProducer__db_cur is not None

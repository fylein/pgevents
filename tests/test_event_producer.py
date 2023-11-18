import json
from unittest import mock
from common.event import base_event
from common.qconnector.rabbitmq_connector import RabbitMQConnector
from producer.event_producer import EventProducer


# Test init
def test_init(producer_init_params):
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


# Test connect
def test_connect(mock_producer, mock_pika_connect, mock_pg_conn):
    mock_producer.connect()

    assert mock_producer._EventProducer__db_conn is not None
    assert mock_producer._EventProducer__db_cur is not None

    mock_pika_connect.assert_called_once()
    mock_pg_conn.assert_called_once()


# Test PGOutputMessageProcessor
def test_pgoutput_msg_processor(mock_producer, relation_payload, insert_payload, update_payload, delete_payload, mock_schema):
    # mock msg.cursor.send_feedback
    mock_msg = mock.Mock()
    mock_msg.cursor.send_feedback = mock.Mock()
    mock_msg.cursor.send_feedback.return_value = None

    mock_producer.publish = mock.Mock()

    # mock msg.payload
    mock_msg.payload = relation_payload.payload
    mock_producer.pgoutput_msg_processor(mock_msg)

    assert mock_producer._EventProducer__table_schemas[16385] == mock_schema

    # mock msg.payload
    mock_msg.payload = insert_payload.payload
    mock_producer.pgoutput_msg_processor(mock_msg)

    assert mock_producer._EventProducer__table_schemas[16385] == mock_schema

    # mock msg.payload
    mock_msg.payload = update_payload.payload
    mock_producer.pgoutput_msg_processor(mock_msg)

    assert mock_producer._EventProducer__table_schemas[16385] == mock_schema

    # mock msg.payload
    mock_msg.payload = delete_payload.payload
    mock_producer.pgoutput_msg_processor(mock_msg)

    assert mock_producer._EventProducer__table_schemas[16385] == mock_schema

    assert mock_msg.cursor.send_feedback.call_count == 4

    assert mock_producer.publish.call_count == 3


# Test Wal2JsonMessageProcessor
def test_wal2json_msg_processor(mock_producer, wal2json_payload):
    mock_msg = mock.Mock()
    mock_msg.payload = json.dumps(wal2json_payload)
    mock_msg.cursor.send_feedback = mock.Mock()
    mock_msg.cursor.send_feedback.return_value = None

    mock_producer.publish = mock.Mock()

    mock_producer.wal2json_msg_processor(mock_msg)

    assert mock_msg.cursor.send_feedback.call_count == 1

    assert mock_producer.publish.call_count == 1

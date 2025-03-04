import json
from unittest import mock

import psycopg2
import pytest

from common.event import base_event
from common.qconnector.rabbitmq_connector import RabbitMQConnector
from pgoutput_parser.update import UpdateMessage
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

    mock_producer._EventProducer__pg_output_plugin = 'wal2json'
    mock_producer.connect()

    assert mock_producer._EventProducer__db_conn is not None
    assert mock_producer._EventProducer__db_cur is not None

    mock_pika_connect.call_count == 2
    mock_pg_conn.call_count == 2


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


# Test publish
def test_publish(mock_producer):
    # mock basic_publish
    mock_producer.qconnector._RabbitMQConnector__rmq_channel = mock.Mock()

    mock_producer.publish(routing_key='test', payload='test')

    mock_producer.qconnector._RabbitMQConnector__rmq_channel.basic_publish.assert_called_once()


# Test start_consuming
def test_start_consuming(mock_producer):
    # mock consume_stream pyscopg2
    mock_producer._EventProducer__db_cur = mock.Mock()
    mock_producer._EventProducer__db_cur.consume_stream = mock.Mock()

    mock_producer.start_consuming()

    mock_producer._EventProducer__db_cur.consume_stream.assert_called_once()

    # Test stream_consumer function
    mock_producer.wal2json_msg_processor = mock.Mock()
    mock_producer.pgoutput_msg_processor = mock.Mock()
    mock_producer.check_shutdown = mock.Mock()

    mock_producer._EventProducer__db_cur.consume_stream.call_args[1]['consume']('test')

    mock_producer._EventProducer__pg_output_plugin = 'wal2json'
    mock_producer._EventProducer__db_cur.consume_stream.call_args[1]['consume']('test')
    mock_producer.wal2json_msg_processor.assert_called_once()

@mock.patch('psycopg2.connect')
def test_drop_replication_slot(mock_pg_conn, mock_producer):
    mock_cursor = mock.Mock()
    mock_connection = mock.Mock()
    
    mock_cursor_cm = mock.MagicMock()
    mock_cursor_cm.__enter__ = mock.Mock(return_value=mock_cursor)
    mock_cursor_cm.__exit__ = mock.Mock(return_value=None)
    
    mock_connection.cursor = mock.Mock(return_value=mock_cursor_cm)
    mock_pg_conn.return_value = mock_connection

    mock_producer._EventProducer__drop_replication_slot()

    mock_pg_conn.assert_called_once_with(
        host=mock_producer._EventProducer__pg_host,
        port=mock_producer._EventProducer__pg_port,
        dbname=mock_producer._EventProducer__pg_database,
        user=mock_producer._EventProducer__pg_user,
        password=mock_producer._EventProducer__pg_password
    )

    mock_cursor.execute.assert_called_once_with(
        "SELECT pg_drop_replication_slot(%s);",
        (mock_producer._EventProducer__pg_replication_slot,)
    )

    mock_connection.commit.assert_called_once()
    mock_connection.close.assert_called_once()


def test_shutdown_cleanup(mock_producer):
    # Mock the necessary components
    mock_producer._EventProducer__db_conn = mock.Mock()
    mock_producer._EventProducer__db_conn.closed = False
    mock_producer._EventProducer__db_cur = mock.Mock()
    mock_producer.qconnector = mock.Mock()
    
    mock_producer._EventProducer__drop_replication_slot = mock.Mock()

    mock_producer.shutdown()

    assert mock_producer._EventProducer__shutdown is True
    mock_producer._EventProducer__db_cur.close.assert_called_once()
    mock_producer._EventProducer__db_conn.close.assert_called_once()
    mock_producer._EventProducer__drop_replication_slot.assert_called_once()
    mock_producer.qconnector.shutdown.assert_called_once()

def test_pgoutput_msg_processor_none_update_message(mock_producer, mock_schema):
    mock_msg = mock.Mock()
    mock_msg.payload = b'U\x00\x00@\x01'
    mock_msg.cursor.send_feedback = mock.Mock()
    mock_msg.data_start = 123

    mock_producer._EventProducer__table_schemas = {
        16385: {
            'relation_id': 16385,
            'table_name': 'public.users',
            'columns': [
                {'name': 'id'},
                {'name': 'full_name'}
            ]
        }
    }
    mock_producer._EventProducer__pg_tables = ['public.users']
    mock_producer.publish = mock.Mock()

    with mock.patch('pgoutput_parser.UpdateMessage.decode_update_message', return_value=None):
        with mock.patch('common.utils.DeserializerUtils.convert_bytes_to_int', return_value=16385):
            mock_producer.pgoutput_msg_processor(mock_msg)

            mock_producer.publish.assert_not_called()
            
            mock_msg.cursor.send_feedback.assert_called_once_with(flush_lsn=mock_msg.data_start)

def test_update_message_returns_none(mock_schema):
    message = b'U\x00\x00@\x01\x00\x02n\x00n'  # Message with NULL values
    
    parser = UpdateMessage(table_name=mock_schema['table_name'], message=message, schema=mock_schema)
    parsed_message = parser.decode_update_message()
    
    assert parsed_message is None


def test_start_consuming_with_shutdown(mock_producer):
    mock_producer._EventProducer__db_cur = mock.Mock()
    mock_producer._EventProducer__db_cur.consume_stream = mock.Mock()
    mock_producer._EventProducer__shutdown = True

    mock_producer.start_consuming()

    callback = mock_producer._EventProducer__db_cur.consume_stream.call_args[1]['consume']
    
    test_msg = mock.Mock()
    test_msg.payload = b'I\x00\x00@\x01'
    
    with pytest.raises(SystemExit):
        callback(test_msg)

def test_shutdown_with_no_db_conn(mock_producer):
    mock_producer._EventProducer__db_conn = None
    mock_producer._EventProducer__db_cur = None
    
    # Setup other required mocks
    mock_producer._EventProducer__drop_replication_slot = mock.Mock()
    mock_producer.qconnector = mock.Mock()

    mock_producer.shutdown()

    assert mock_producer._EventProducer__shutdown is True

    mock_producer._EventProducer__drop_replication_slot.assert_called_once()
    mock_producer.qconnector.shutdown.assert_called_once()


def test_shutdown_with_db_conn_errors(mock_producer):
    """Test shutdown with database connection errors"""
    db_conn = mock.Mock()
    db_conn.closed = False
    db_cur = mock.Mock()
    
    mock_producer._EventProducer__db_conn = db_conn
    mock_producer._EventProducer__db_cur = db_cur
    
    mock_producer._EventProducer__drop_replication_slot = mock.Mock()
    mock_producer._EventProducer__drop_replication_slot.side_effect = Exception("Failed to drop replication slot")
    mock_producer.qconnector = mock.Mock()
    mock_producer.qconnector.shutdown.side_effect = Exception("Failed to shutdown queue")

    mock_producer.shutdown()

    assert mock_producer._EventProducer__shutdown is True

    db_cur.close.assert_called_once()
    db_conn.close.assert_called_once()
    mock_producer._EventProducer__drop_replication_slot.assert_called_once()
    mock_producer.qconnector.shutdown.assert_called_once()

def test_create_replication_slot_operational_error(mock_producer):
    """Test operational error handling in create_replication_slot"""
    mock_producer._EventProducer__db_cur = mock.Mock()
    mock_producer._EventProducer__db_cur.execute.side_effect = psycopg2.errors.OperationalError("Test error")
    
    with pytest.raises(psycopg2.errors.OperationalError) as exc_info:
        mock_producer._EventProducer__create_replication_slot()
    
    assert "Operational error during initialization" in str(exc_info.value)

def test_start_consuming_exception_handling(mock_producer):
    """Test exception handling in start_consuming"""
    mock_producer._EventProducer__db_cur = mock.Mock()
    mock_producer._EventProducer__db_cur.consume_stream.side_effect = Exception("Test error")
    mock_producer._EventProducer__shutdown = False
    
    with pytest.raises(Exception):
        mock_producer.start_consuming()

def test_drop_replication_slot_exception(mock_producer):
    """Test exception handling in drop_replication_slot"""
    with mock.patch('psycopg2.connect') as mock_connect:
        mock_cursor = mock.Mock()
        mock_cursor.execute.side_effect = Exception("Test error")
        
        mock_connection = mock.Mock()
        mock_connection.cursor.return_value = mock.MagicMock(
            __enter__=mock.Mock(return_value=mock_cursor),
            __exit__=mock.Mock(return_value=None)
        )
        
        mock_connect.return_value = mock_connection
        
        mock_producer._EventProducer__drop_replication_slot()
        
        mock_connection.close.assert_called_once()

def test_start_consuming_shutdown_exception(mock_producer):
    """Test shutdown exception handling in start_consuming"""
    mock_producer._EventProducer__db_cur = mock.Mock()
    mock_producer._EventProducer__db_cur.consume_stream.side_effect = Exception("Test error")
    mock_producer._EventProducer__shutdown = True

def test_shutdown_with_connection_error(mock_producer):
    """Test connection error handling during shutdown"""
    mock_producer._EventProducer__db_conn = mock.Mock()
    mock_producer._EventProducer__db_conn.closed = False
    mock_producer._EventProducer__db_cur = mock.Mock()
    mock_producer._EventProducer__db_cur.close.side_effect = Exception("Test error")
    
    mock_producer._EventProducer__drop_replication_slot = mock.Mock()
    mock_producer.qconnector = mock.Mock()
    
    mock_producer.shutdown()
    
    assert mock_producer._EventProducer__shutdown is True
    mock_producer._EventProducer__db_cur.close.assert_called_once()
    mock_producer._EventProducer__drop_replication_slot.assert_called_once()
    mock_producer.qconnector.shutdown.assert_called_once()

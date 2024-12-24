import json
import click
from click.testing import CliRunner
from unittest import mock
import pytest

from common.event import base_event
from common.qconnector.rabbitmq_connector import RabbitMQConnector
from common.utils import validate_db_configs
from producer.event_producer import EventProducer
from producer.main import producer_multiple_dbs


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


def test_valid_single_db_config():
    config = [{
        'pg_host': 'localhost',
        'pg_port': 5432,
        'pg_database': 'test_db',
        'pg_user': 'postgres',
        'pg_password': 'secret',
        'pg_tables': 'public.users',
        'pg_replication_slot': 'test_slot'
    }]
    assert validate_db_configs(config) == None


def test_valid_multiple_db_configs():
    configs = [
        {
            'pg_host': 'localhost',
            'pg_port': 5432,
            'pg_database': 'db1',
            'pg_user': 'postgres',
            'pg_password': 'secret',
            'pg_tables': 'public.users',
            'pg_replication_slot': 'slot1'
        },
        {
            'pg_host': 'localhost',
            'pg_port': 5433,
            'pg_database': 'db2',
            'pg_user': 'postgres',
            'pg_password': 'secret',
            'pg_tables': 'public.orders',
            'pg_replication_slot': 'slot2'
        }
    ]
    assert validate_db_configs(configs) == None


def test_empty_config_list():
    with pytest.raises(click.BadParameter, match="db_configs cannot be empty"):
        validate_db_configs([])


def test_non_list_input():
    with pytest.raises(click.BadParameter, match="db_configs must be a list"):
        validate_db_configs({'some': 'dict'})


def test_missing_required_field():
    config = [{
        'pg_host': 'localhost',
        'pg_port': 5432
    }]
    with pytest.raises(click.BadParameter, match="Missing required field"):
        validate_db_configs(config)


def test_invalid_field_type():
    config = [{
        'pg_host': 'localhost',
        'pg_port': '5432',
        'pg_database': 'test_db',
        'pg_user': 'postgres',
        'pg_password': 'secret',
        'pg_tables': 'public.users',
        'pg_replication_slot': 'test_slot'
    }]
    with pytest.raises(click.BadParameter, match="must be of type int"):
        validate_db_configs(config)


def test_invalid_port_range():
    config = [{
        'pg_host': 'localhost',
        'pg_port': 80,
        'pg_database': 'test_db',
        'pg_user': 'postgres',
        'pg_password': 'secret',
        'pg_tables': 'public.users',
        'pg_replication_slot': 'test_slot'
    }]
    with pytest.raises(click.BadParameter, match="Invalid port number"):
        validate_db_configs(config)


def test_invalid_table_format():
    config = [{
        'pg_host': 'localhost',
        'pg_port': 5432,
        'pg_database': 'test_db',
        'pg_user': 'postgres',
        'pg_password': 'secret',
        'pg_tables': 'invalid_format',
        'pg_replication_slot': 'test_slot'
    }]
    with pytest.raises(click.BadParameter, match="Invalid table format"):
        validate_db_configs(config)


@pytest.fixture
def valid_db_configs():
    return json.dumps([{
        'pg_host': 'database',
        'pg_port': 5432,
        'pg_database': 'dummy',
        'pg_user': 'postgres',
        'pg_password': 'postgres',
        'pg_tables': 'public.users',
        'pg_replication_slot': 'events'
    }])

def test_producer_multiple_dbs_success(valid_db_configs):
    runner = CliRunner()
    result = runner.invoke(producer_multiple_dbs, [
        '--db_configs', valid_db_configs,
        '--rabbitmq_url', 'amqp://admin:password@rabbitmq:5672/?heartbeat=0',
        '--rabbitmq_exchange', 'pgevents_exchange'
    ])

    assert result.exit_code == 0

def test_producer_multiple_dbs_invalid_json():
    runner = CliRunner()
    result = runner.invoke(producer_multiple_dbs, [
        '--db_configs', 'invalid-json',
        '--rabbitmq_url', 'amqp://admin:password@rabbitmq:5672/?heartbeat=0',
        '--rabbitmq_exchange', 'pgevents_exchange'
    ])

    assert "db_configs must be a valid JSON string" in str(result.__dict__)

@mock.patch('producer.main.validate_db_configs')
def test_producer_multiple_dbs_invalid_config(mock_validate, valid_db_configs):
    mock_validate.side_effect = click.BadParameter("Invalid config")

    runner = CliRunner()
    result = runner.invoke(producer_multiple_dbs, [
        '--db_configs', valid_db_configs,
        '--rabbitmq_url', 'amqp://admin:password@rabbitmq:5672/?heartbeat=0',
        '--rabbitmq_exchange', 'pgevents_exchange'
    ])

    assert "Invalid config" in str(result.__dict__)

def test_producer_multiple_dbs_common_kwargs(valid_db_configs):
    runner = CliRunner()
    result = runner.invoke(producer_multiple_dbs, [
        '--db_configs', valid_db_configs,
        '--pg_output_plugin', 'test_plugin',
        '--pg_publication_name', 'test_pub',
        '--rabbitmq_url', 'amqp://admin:password@rabbitmq:5672/?heartbeat=0',
        '--rabbitmq_exchange', 'pgevents_exchange'
    ])

    assert result.exit_code == 0

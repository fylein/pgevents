from unittest import mock
import pytest
from common.event import base_event

from common.qconnector import RabbitMQConnector
from common import log

from producer.event_producer import EventProducer
from consumer.event_consumer import EventConsumer

logger = log.get_logger(__name__)


@pytest.fixture
def producer_init_params():
    return {
        'qconnector_cls': RabbitMQConnector,
        'event_cls': base_event.BaseEvent,
        'pg_host': 'localhost',
        'pg_port': 5432,
        'pg_database': 'test',
        'pg_user': 'test',
        'pg_password': 'test',
        'pg_replication_slot': 'test',
        'pg_output_plugin': 'pgoutput',
        'pg_tables': 'public.users',
        'pg_publication_name': 'test',
        'rabbitmq_url': 'amqp://admin:password@rabbitmq:5672/?heartbeat=0',
        'rabbitmq_exchange': 'test'
    }


@pytest.fixture
def mock_producer(producer_init_params):
    return EventProducer(**producer_init_params)


@pytest.fixture
def mock_pika_connect():
    with mock.patch('pika.BlockingConnection') as mock_pika:
        yield mock_pika


@pytest.fixture
def mock_pg_conn():
    with mock.patch('psycopg2.connect') as mock_pg:
        yield mock_pg


@pytest.fixture
def mock_schema():
    return {
        'relation_id': 16385,
        'table_name': 'public.users',
        'columns': [
            {'name': 'id'},
            {'name': 'full_name'},
            {'name': 'company'},
            {'name': 'created_at'},
            {'name': 'updated_at'}
        ]
    }

@pytest.fixture
def wal2json_payload():
    return {"action":"D","lsn":"0/1671850","schema":"public","table":"users","identity":[{"name":"id","type":"integer","value":2},{"name":"full_name","type":"text","value":"Geezer Butler"},{"name":"company","type":"jsonb","value":"{\"name\": \"Fyle\"}"},{"name":"created_at","type":"timestamp with time zone","value":"2023-11-17 13:35:09.471909+00"},{"name":"updated_at","type":"timestamp with time zone","value":"2023-11-17 13:35:09.471909+00"}]}

# Class to hold payload data
class OutputData:
    payload = None
    data_start = 124122


# Fixture for relation payload
@pytest.fixture
def relation_payload():
    data = OutputData()
    data.payload = b'R\x00\x00@\x01public\x00users\x00f\x00\x05\x01id\x00\x00\x00\x00\x17\xff\xff\xff\xff\x01full_name\x00\x00\x00\x00\x19\xff\xff\xff\xff\x01company\x00\x00\x00\x0e\xda\xff\xff\xff\xff\x01created_at\x00\x00\x00\x04\xa0\xff\xff\xff\xff\x01updated_at\x00\x00\x00\x04\xa0\xff\xff\xff\xff'
    return data


# Fixture for expected relation response
@pytest.fixture
def relation_response():
    return {
        'relation_id': 16385,
        'table_name': 'public.users',
        'columns': [
            {'name': 'id'},
            {'name': 'full_name'},
            {'name': 'company'},
            {'name': 'created_at'},
            {'name': 'updated_at'}
        ]
    }


# Fixture for insert payload
@pytest.fixture
def insert_payload():
    data = OutputData()
    data.payload = b'I\x00\x00@\x01N\x00\x05t\x00\x00\x00\x011t\x00\x00\x00\x04Miket\x00\x00\x00\x10{"name": "Fyle"}t\x00\x00\x00\x1d2023-11-17 13:44:14.700844+00t\x00\x00\x00\x1d2023-11-17 13:44:14.700844+00'
    return data


# Fixture for expected insert response
@pytest.fixture
def insert_response():
    return {
        'table_name': 'public.users',
        'new': {
            'id': '1',
            'full_name': 'Mike',
            'company': '{"name": "Fyle"}',
            'created_at': '2023-11-17 13:44:14.700844+00',
            'updated_at': '2023-11-17 13:44:14.700844+00',
        },
        'id': '1',
        'old': {},
        'diff': {
            'id': '1',
            'full_name': 'Mike',
            'company': '{"name": "Fyle"}',
            'created_at': '2023-11-17 13:44:14.700844+00',
            'updated_at': '2023-11-17 13:44:14.700844+00',
        },
        'action': 'I',
        'recorded_at': '2023-11-17 13:44:14.700844+00'
    }

# Fixture for update payload
@pytest.fixture
def update_payload():
    data = OutputData()
    data.payload = b'U\x00\x00@\x01O\x00\x05t\x00\x00\x00\x011t\x00\x00\x00\x04Miket\x00\x00\x00\x10{"name": "Fyle"}t\x00\x00\x00\x1d2023-11-17 13:44:14.700844+00t\x00\x00\x00\x1d2023-11-17 13:44:14.700844+00N\x00\x05t\x00\x00\x00\x011t\x00\x00\x00\x05Mylest\x00\x00\x00\x10{"name": "Fyle"}t\x00\x00\x00\x1d2023-11-17 13:44:14.700844+00t\x00\x00\x00\x1d2023-11-17 13:44:14.700844+00'
    return data

# Fixture for expected update response
@pytest.fixture
def update_response():
    return  {
        'table_name': 'public.users',
        'id': '1',
        'old': {
            'id': '1',
            'full_name': 'Mike',
            'company': '{"name": "Fyle"}',
            'created_at': '2023-11-17 13:44:14.700844+00',
            'updated_at': '2023-11-17 13:44:14.700844+00',
        },
        'new': {
            'id': '1',
            'full_name': 'Myles',
            'company': '{"name": "Fyle"}',
            'created_at': '2023-11-17 13:44:14.700844+00',
            'updated_at': '2023-11-17 13:44:14.700844+00',
            
        },
        'diff': {
            'full_name': 'Myles'
        },
        'action': 'U',
        'recorded_at': '2023-11-17 13:44:14.700844+00'
    }

# Fixture for delete payload
@pytest.fixture
def delete_payload():
    data = OutputData()
    data.payload = b'D\x00\x00@\x01O\x00\x05t\x00\x00\x00\x012t\x00\x00\x00\rGeezer Butlert\x00\x00\x00\x10{"name": "Fyle"}t\x00\x00\x00\x1d2023-11-17 13:35:09.471909+00t\x00\x00\x00\x1d2023-11-17 13:35:09.471909+00'
    return data

# Fixture for expected delete response
@pytest.fixture
def delete_response():
    return {
        'table_name': 'public.users',
        'action': 'D',
        'old': {
            'id': '2',
            'full_name': 'Geezer Butler', 
            'company': '{"name": "Fyle"}',
            'created_at': '2023-11-17 13:35:09.471909+00',
            'updated_at': '2023-11-17 13:35:09.471909+00',
        },
        'id': '2',
        'new': {},
        'diff': {},
        'recorded_at': '2023-11-17 13:35:09.471909+00'
    }


@pytest.fixture
def event_consumer_init_params():
    return {
        'qconnector_cls': RabbitMQConnector,
        'event_cls': base_event.BaseEvent,
        'rabbitmq_url': 'amqp://admin:password@rabbitmq:5672/?heartbeat=0',
        'rabbitmq_exchange': 'test',
        'queue_name': 'test',
        'binding_keys': 'public.users'
    }


@pytest.fixture
def mock_consumer(event_consumer_init_params):
    return EventConsumer(**event_consumer_init_params)


@pytest.fixture
def mock_event(update_response):
    event = base_event.BaseEvent()
    event.from_dict(update_response)
    return event

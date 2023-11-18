import os
import psycopg2
import pytest

from common.qconnector import RabbitMQConnector
from common import log

logger = log.get_logger(__name__)


@pytest.fixture(scope='function')
def db_conn():
    db_connection = psycopg2.connect(
        user=os.environ['PGUSER'],
        password=os.environ['PGPASSWORD'],
        host=os.environ['PGHOST'],
        port=os.environ['PGPORT'],
        dbname=os.environ['PGDATABASE']
    )

    yield db_connection
    db_connection.close()


@pytest.fixture(scope='session')
def rmq_conn():
    rmq_connector = RabbitMQConnector(
        rabbitmq_url=os.environ['RABBITMQ_URL'],
        rabbitmq_exchange=os.environ['RABBITMQ_EXCHANGE'],
        queue_name='PRODUCER_TEST_QUEUE',
        binding_keys='#'
    )

    rmq_connector.connect()
    return rmq_connector


@pytest.fixture
def mocked_schema():
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
            'updated_at': '2023-11-17 13:44:14.700844+00'
        },
        'id': '1',
        'old': {},
        'diff': {
            'id': '1',
            'full_name': 'Mike',
            'company': '{"name": "Fyle"}',
            'created_at': '2023-11-17 13:44:14.700844+00',
            'updated_at': '2023-11-17 13:44:14.700844+00'
        },
        'action': 'I'
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
            'updated_at': '2023-11-17 13:44:14.700844+00'
        },
        'new': {
            'id': '1',
            'full_name': 'Myles',
            'company': '{"name": "Fyle"}',
            'created_at': '2023-11-17 13:44:14.700844+00',
            'updated_at': '2023-11-17 13:44:14.700844+00'
        },
        'diff': {
            'full_name': 'Myles'
        },
        'action': 'U'
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
            'updated_at': '2023-11-17 13:35:09.471909+00'
        },
        'id': '2',
        'new': {},
        'diff': {}
    }

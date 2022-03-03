import os
import psycopg2
import pytest

from src.qconnector.rabbitmq_connector import RabbitMQConnector
from src.common import log

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

import pytest
import pika
from common.qconnector.rabbitmq_connector import RabbitMQConnector
from unittest import mock

@pytest.fixture
def rabbitmq_connector():
    connector = RabbitMQConnector(
        rabbitmq_url='amqp://guest:guest@localhost:5672/',
        rabbitmq_exchange='test_exchange',
        queue_name='test_queue',
        binding_keys='test.#'
    )
    connector._RabbitMQConnector__rmq_conn = mock.Mock()
    connector._RabbitMQConnector__rmq_channel = mock.Mock()
    return connector

def test_disconnect(rabbitmq_connector):
    """Test disconnect method"""
    rabbitmq_connector.disconnect()
    rabbitmq_connector._RabbitMQConnector__rmq_conn.close.assert_called_once()

def test_consume_stream(rabbitmq_connector):
    """Test consume_stream method"""
    callback = mock.Mock()
    
    rabbitmq_connector.consume_stream(callback)
    
    rabbitmq_connector._RabbitMQConnector__rmq_channel.basic_consume.assert_called_once()
    args, kwargs = rabbitmq_connector._RabbitMQConnector__rmq_channel.basic_consume.call_args
    
    assert kwargs['queue'] == 'test_queue'
    assert kwargs['auto_ack'] is True
    
    stream_consumer = kwargs['on_message_callback']
    
    ch = mock.Mock()
    method = mock.Mock()
    method.routing_key = 'test.key'
    properties = mock.Mock()
    body = b'compressed_test_body'
    
    with mock.patch('common.qconnector.rabbitmq_connector.decompress', return_value='decompressed_data'):
        stream_consumer(ch, method, properties, body)
        
        callback.assert_called_once_with(
            routing_key='test.key',
            payload='decompressed_data'
        )
    
    rabbitmq_connector._RabbitMQConnector__rmq_channel.start_consuming.assert_called_once()

def test_consume_all(rabbitmq_connector):
    """Test consume_all method"""
    method1 = mock.Mock()
    method1.routing_key = 'test.key1'
    method2 = mock.Mock()
    method2.routing_key = 'test.key2'
    
    rabbitmq_connector._RabbitMQConnector__rmq_channel.basic_get.side_effect = [
        (method1, None, b'compressed_body1'),
        (method2, None, b'compressed_body2'),
        (None, None, None)
    ]
    
    with mock.patch('common.qconnector.rabbitmq_connector.decompress') as mock_decompress:
        mock_decompress.side_effect = ['decompressed1', 'decompressed2']
        
        result = rabbitmq_connector.consume_all()
        
        assert result == [
            ('test.key1', 'decompressed1'),
            ('test.key2', 'decompressed2')
        ]
        
        assert rabbitmq_connector._RabbitMQConnector__rmq_channel.basic_get.call_count == 3
        
        mock_decompress.assert_has_calls([
            mock.call(b'compressed_body1'),
            mock.call(b'compressed_body2')
        ])

def test_consume_all_empty_queue(rabbitmq_connector):
    """Test consume_all method with empty queue"""
    rabbitmq_connector._RabbitMQConnector__rmq_channel.basic_get.return_value = (None, None, None)
    
    result = rabbitmq_connector.consume_all()
    
    assert result == []
    
    rabbitmq_connector._RabbitMQConnector__rmq_channel.basic_get.assert_called_once_with(
        rabbitmq_connector._RabbitMQConnector__queue_name, True
    ) 

from unittest import mock
from common.event import base_event
from common.qconnector.rabbitmq_connector import RabbitMQConnector
from consumer.event_consumer import EventConsumer


# Test init
def test_init(event_consumer_init_params):
    c = EventConsumer(**event_consumer_init_params)

    assert c.qconnector_cls == RabbitMQConnector
    assert c.event_cls == base_event.BaseEvent
    assert c.qconnector._RabbitMQConnector__rabbitmq_url == 'amqp://admin:password@rabbitmq:5672/?heartbeat=0'
    assert c.qconnector._RabbitMQConnector__rabbitmq_exchange == 'test'


# Test connect
def test_connect(mock_consumer, mock_pika_connect):
    mock_consumer.connect()
    mock_pika_connect.assert_called_once()


# Test start_consuming
def test_start_consuming(mock_consumer):
    # mock basic_consume
    mock_consumer.qconnector.consume_stream = mock.Mock()
    mock_consumer.start_consuming()

    mock_consumer.qconnector.consume_stream.assert_called_once()

    # test stream_consumer function
    mock_consumer.process_message = mock.Mock()
    mock_consumer.check_shutdown = mock.Mock()
    mock_consumer.qconnector.consume_stream.call_args[1]['callback_fn']('test', '{"test": "test"}')

    mock_consumer.process_message.assert_called_once()
    mock_consumer.check_shutdown.assert_called_once()


# Test process_message
def test_process_message(mock_consumer, mock_event):
    with mock.patch('logging.Logger.info') as mock_logger:
        mock_consumer.process_message('test', mock_event, 1)
        mock_logger.call_count == 3


# Test shutdown
def test_shutdown(mock_consumer):
    mock_consumer.shutdown()
    assert mock_consumer._EventConsumer__shutdown is True


# Test check_shutdown
def test_check_shutdown(mock_consumer):
    mock_consumer._EventConsumer__shutdown = True
    mock_consumer.qconnector.check_shutdown = mock.Mock()
    mock_consumer.check_shutdown()
    mock_consumer.qconnector.check_shutdown.assert_called_once()

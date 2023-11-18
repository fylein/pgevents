import pytest
from unittest import mock

from pgoutput_parser import (
    InsertMessage,
    UpdateMessage,
    DeleteMessage,
    RelationMessage
)
from pgoutput_parser.base import BaseMessage


# Test InsertMessage decoding
def test_insert(insert_payload, insert_response, mocked_schema):

    parser = InsertMessage(table_name=mocked_schema['table_name'], message=insert_payload.payload, schema=mocked_schema)
    parsed_message = parser.decode_insert_message()

    assert parsed_message == insert_response


# Test UpdateMessage decoding
def test_update(update_payload, update_response, mocked_schema):
    parser = UpdateMessage(table_name=mocked_schema['table_name'], message=update_payload.payload, schema=mocked_schema)
    parsed_message = parser.decode_update_message()

    assert parsed_message == update_response


# Test DeleteMessage decoding
def test_delete(delete_payload, delete_response, mocked_schema):
    parser = DeleteMessage(table_name=mocked_schema['table_name'], message=delete_payload.payload, schema=mocked_schema)
    parsed_message = parser.decode_delete_message()

    assert parsed_message == delete_response


# Test RelationMessage decoding
def test_relation(relation_payload, relation_response):
    parser = RelationMessage(table_name=None, message=relation_payload.payload, schema=None)
    parsed_message = parser.decode_relation_message()

    assert parsed_message == relation_response


# Test BaseMessage for NotImplementedError
def test_base_not_implemented_methods():
    parser = BaseMessage(table_name=None, message=b'123', schema=None)

    with pytest.raises(NotImplementedError) as excinfo:
        parser.decode_insert_message()
    assert 'This method should be overridden by subclass' in str(excinfo.value)

    with pytest.raises(NotImplementedError) as excinfo:
        parser.decode_update_message()
    assert 'This method should be overridden by subclass' in str(excinfo.value)

    with pytest.raises(NotImplementedError) as excinfo:
        parser.decode_delete_message()
    assert 'This method should be overridden by subclass' in str(excinfo.value)

    with pytest.raises(NotImplementedError) as excinfo:
        parser.decode_relation_message()
    assert 'This method should be overridden by subclass' in str(excinfo.value)


# Test BaseMessage for decode_tuple for 'u' and 'n' types
def test_decode_tuple_null_and_unchanged():
    base_message_instance = BaseMessage(table_name=None, message=b'123', schema=None)
    # Replace with actual class instantiation if needed
    base_message_instance.schema = {'columns': [{'name': 'col1'}, {'name': 'col2'}]}

    # Mock the methods
    base_message_instance.read_int16 = mock.MagicMock(return_value=2)
    base_message_instance.read_utf_8 = mock.MagicMock(side_effect=['n', 'u'])
    base_message_instance.read_int32 = mock.MagicMock()  # This won't be called

    # Call the method
    result = base_message_instance.decode_tuple()

    # Assertions
    assert result == {'col1': None, 'col2': None}

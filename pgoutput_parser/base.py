import io
from typing import Any, Dict

from common.utils import DeserializerUtils, get_utc_now
from common.log import get_logger


logger = get_logger(__name__)


class BaseMessage:
    """Base class for decoding PostgreSQL logical replication messages."""

    def __init__(self, table_name: str, message: bytes, schema: dict) -> None:
        """
        Initialize the BaseMessage instance.

        :param table_name: The name of the table being replicated.
        :param message: The raw message payload from the replication stream.
        """
        self.message = message
        self.table_name = table_name
        self.buffer = io.BytesIO(message)
        self.message_type = self.read_utf_8(length=1)
        self.relation_id = self.read_int32()
        self.schema = schema
        self.recorded_at = get_utc_now()

    def read_int8(self) -> int:
        """Read an 8-bit integer from the buffer."""
        return DeserializerUtils.convert_bytes_to_int(self.buffer.read(1))

    def read_int16(self) -> int:
        """Read a 16-bit integer from the buffer."""
        return DeserializerUtils.convert_bytes_to_int(self.buffer.read(2))

    def read_int32(self) -> int:
        """Read a 32-bit integer from the buffer."""
        return DeserializerUtils.convert_bytes_to_int(self.buffer.read(4))

    def read_utf_8(self, length: int) -> str:
        """Read a string of a given length from the buffer."""
        return DeserializerUtils.convert_bytes_to_utf8(self.buffer.read(length))

    def read_string(self) -> str:
        """Read a null-terminated string from the buffer."""
        output = bytearray()
        while (next_char := self.buffer.read(1)) != b"\x00":
            output += next_char

        return DeserializerUtils.convert_bytes_to_utf8(output)

    @staticmethod
    def calculate_diff(old_tuple_values: Dict[str, Any], new_tuple_values: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        Calculate the difference between old and new tuple values.

        :param old_tuple_values: Dictionary containing old tuple values.
        :param new_tuple_values: Dictionary containing new tuple values.
        :return: A dictionary containing the differences.
        """
        diff = {}

        for key in new_tuple_values.keys():
            if old_tuple_values.get(key) != new_tuple_values.get(key):
                diff[key] = new_tuple_values[key]
        return diff

    def decode_tuple(self) -> dict:
        """
        Decode a tuple from the message.

        :return: A dictionary containing the decoded data.
        """
        n_columns = self.read_int16()

        data = {}
        columns = self.schema['columns']

        for i in range(n_columns):
            col_type = self.read_utf_8(length=1)

            if col_type == 'n':
                data[columns[i]['name']] = None
            elif col_type == 'u':
                data[columns[i]['name']] = None
            elif col_type == 't':
                length = self.read_int32()
                value = self.read_utf_8(length=length)
                data[columns[i]['name']] = value

        return data

    def decode_insert_message(self):
        """Placeholder for decoding insert messages. Should be overridden by subclass."""
        raise NotImplementedError('This method should be overridden by subclass')

    def decode_update_message(self):
        """Placeholder for decoding update messages. Should be overridden by subclass."""
        raise NotImplementedError('This method should be overridden by subclass')

    def decode_delete_message(self):
        """Placeholder for decoding delete messages. Should be overridden by subclass."""
        raise NotImplementedError('This method should be overridden by subclass')
    
    def decode_relation_message(self):
        """Placeholder for decoding relation messages. Should be overridden by subclass."""
        raise NotImplementedError('This method should be overridden by subclass')

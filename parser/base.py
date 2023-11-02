import io
import logging
from common.utils import Utils


class BaseMessage:
    """Base class for decoding PostgreSQL logical replication messages."""

    def __init__(self, message: bytes, cursor) -> None:
        """
        Initialize the BaseMessage instance.

        :param message: The raw message payload from the replication stream.
        :param cursor: A psycopg2 cursor object for database operations.
        """
        self.message = message
        self.buffer = io.BytesIO(message)
        self.message_type = self.read_string(length=1)
        self.relation_id = self.read_int32()
        self.cursor = cursor
        self.schema = self.get_schema()

    def read_int16(self) -> int:
        """Read a 16-bit integer from the buffer."""
        return Utils.convert_bytes_to_int(self.buffer.read(2))

    def read_int32(self) -> int:
        """Read a 32-bit integer from the buffer."""
        return Utils.convert_bytes_to_int(self.buffer.read(4))

    def read_string(self, length: int) -> str:
        """Read a string of a given length from the buffer."""
        return Utils.convert_bytes_to_utf8(self.buffer.read(length))

    def decode_tuple(self) -> dict:
        """
        Decode a tuple from the message.

        :return: A dictionary containing the decoded data.
        """
        n_columns = self.read_int16()
        logging.debug(f'Number of columns: {n_columns}')

        data = {}
        columns = self.schema['columns']

        for i in range(n_columns):
            col_type = self.read_string(length=1)
            logging.debug(f'Column type: {col_type}')

            if col_type == 'n':
                logging.debug('NULL')
                data[columns[i]['name']] = None
            elif col_type == 'u':
                logging.debug('Unchanged TOASTed value')
                data[columns[i]['name']] = None
            elif col_type == 't':
                length = self.read_int32()
                value = self.read_string(length=length)
                logging.debug(f'Text: {value}')
                data[columns[i]['name']] = value

        return data

    def get_schema(self) -> dict:
        """
        Retrieve the schema for the relation.

        :return: A dictionary containing the schema information.
        """
        relation_id = self.relation_id
        logging.debug(f'Relation ID: {relation_id}')

        schema = {
            'relation_id': relation_id,
            'columns': []
        }

        self.cursor.execute(
            f'SELECT attname, atttypid FROM pg_attribute WHERE attrelid = {relation_id} AND attnum > 0;'
        )

        for column in self.cursor.fetchall():
            schema['columns'].append({
                'name': column[0],
                'type': column[1]
            })

        return schema

    def decode_insert_message(self):
        """Placeholder for decoding insert messages. Should be overridden by subclass."""
        raise NotImplementedError('This method should be overridden by subclass')

    def decode_update_message(self):
        """Placeholder for decoding update messages. Should be overridden by subclass."""
        raise NotImplementedError('This method should be overridden by subclass')

    def decode_delete_message(self):
        """Placeholder for decoding delete messages. Should be overridden by subclass."""
        raise NotImplementedError('This method should be overridden by subclass')

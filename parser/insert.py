import logging
from .base import BaseMessage


class InsertMessage(BaseMessage):
    """Class for decoding PostgreSQL logical replication insert messages."""

    def decode_insert_message(self) -> dict:
        """
        Decode an insert message from the replication stream.

        :return: A dictionary containing the decoded insert message.
        """
        if self.message_type == 'I':
            message_type = self.message_type
            relation_id = self.relation_id
            new_tuple = self.read_string(length=1)
            new_tuple_values = self.decode_tuple()

            logging.debug(f'Message type: {message_type}')
            logging.debug(f'Relation ID: {relation_id}')
            logging.debug(f'New tuple: {new_tuple}')
            logging.debug(f'New tuple values: {new_tuple_values}')

            return {
                'message_type': message_type,
                'relation_id': relation_id,
                'new': new_tuple_values
            }

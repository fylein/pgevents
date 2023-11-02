import logging
from .base import BaseMessage


class DeleteMessage(BaseMessage):
    """Class for decoding PostgreSQL logical replication delete messages."""

    def decode_delete_message(self) -> dict:
        """
        Decode a delete message from the replication stream.

        :return: A dictionary containing the decoded delete message.
        """
        if self.message_type == 'D':
            message_type = self.message_type
            relation_id = self.relation_id
            old_tuple = self.read_string(length=1)
            old_tuple_values = self.decode_tuple()

            logging.debug(f'Message type: {message_type}')
            logging.debug(f'Relation ID: {relation_id}')
            logging.debug(f'Old tuple: {old_tuple}')

            return {
                'message_type': message_type,
                'relation_id': relation_id,
                'old': old_tuple_values
            }

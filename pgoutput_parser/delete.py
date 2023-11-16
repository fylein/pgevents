from common.log import get_logger

from .base import BaseMessage


logger = get_logger(__name__)


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
            old_tuple = self.read_utf_8(length=1)
            old_tuple_values = self.decode_tuple()

            logger.debug(f'Message type: {message_type}')
            logger.debug(f'Relation ID: {relation_id}')
            logger.debug(f'Old tuple: {old_tuple}')

            return {
                'table_name': self.table_name,
                'action': message_type,
                'old': old_tuple_values,
                'id': old_tuple_values['id'],
                'new': {},
                'diff': {}
            }

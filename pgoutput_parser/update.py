from common.log import get_logger

from .base import BaseMessage


logger = get_logger(__name__)



class UpdateMessage(BaseMessage):
    """Class for decoding PostgreSQL logical replication update messages."""

    def decode_update_message(self) -> dict:
        """
        Decode an update message from the replication stream.

        :return: A dictionary containing the decoded update message.
        """
        if self.message_type == 'U':
            message_type = self.message_type
            relation_id = self.relation_id

            old_tuple = self.read_utf_8(length=1)
            old_tuple_values = self.decode_tuple()
            new_tuple = self.read_utf_8(length=1)
            new_tuple_values = self.decode_tuple()

            logger.debug(f'Message type: {message_type}')
            logger.debug(f'Relation ID: {relation_id}')
            logger.debug(f'Old tuple: {old_tuple}')
            logger.debug(f'New tuple: {new_tuple}')
            logger.debug(f'Old tuple values: {old_tuple_values}')
            logger.debug(f'New tuple values: {new_tuple_values}')


            return {
                'table_name': self.table_name,
                'id': new_tuple_values.get('id') or old_tuple_values.get('id'),
                'old': old_tuple_values,
                'new': new_tuple_values,
                'diff': self.calculate_diff(old_tuple_values, new_tuple_values),
                'action': self.message_type
            }

from common.log import get_logger

from .base import BaseMessage
from typing import Dict, Any


logger = get_logger(__name__)



class UpdateMessage(BaseMessage):
    """Class for decoding PostgreSQL logical replication update messages."""

    @staticmethod
    def __calculate_diff(old_tuple_values: Dict[str, Any], new_tuple_values: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        Calculate the difference between old and new tuple values.

        :param old_tuple_values: Dictionary containing old tuple values.
        :param new_tuple_values: Dictionary containing new tuple values.
        :return: A dictionary containing the differences.
        """
        diff = {}

        for key in old_tuple_values.keys():
            if old_tuple_values[key] != new_tuple_values[key]:
                diff[key] = {
                    'old_value': old_tuple_values[key],
                    'new_value': new_tuple_values[key]
                }
        return diff

    def decode_update_message(self) -> Dict[str, Any]:
        """
        Decode an update message from the replication stream.

        :return: A dictionary containing the decoded update message.
        """
        if self.message_type == 'U':
            message_type = self.message_type
            relation_id = self.relation_id

            old_tuple = self.read_string(length=1)
            old_tuple_values = self.decode_tuple()
            new_tuple = self.read_string(length=1)
            new_tuple_values = self.decode_tuple()

            logger.debug(f'Message type: {message_type}')
            logger.debug(f'Relation ID: {relation_id}')
            logger.debug(f'Old tuple: {old_tuple}')
            logger.debug(f'New tuple: {new_tuple}')

            logger.debug(f'Old tuple values: {old_tuple_values}')
            logger.debug(f'New tuple values: {new_tuple_values}')

            return {
                'table_name': self.table_name,
                'id': new_tuple_values['id'],
                'old': old_tuple_values,
                'new': new_tuple_values,
                'diff': self.__calculate_diff(old_tuple_values, new_tuple_values),
                'action': self.message_type
            }

import logging
from .base import BaseMessage
from typing import Dict, Any


class UpdateMessage(BaseMessage):
    """Class for decoding PostgreSQL logical replication update messages."""

    @staticmethod
    def calculate_diff(old_tuple_values: Dict[str, Any], new_tuple_values: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
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

            logging.debug(f'Message type: {message_type}')
            logging.debug(f'Relation ID: {relation_id}')
            logging.debug(f'Old tuple: {old_tuple}')
            logging.debug(f'New tuple: {new_tuple}')

            logging.debug(f'Old tuple values: {old_tuple_values}')
            logging.debug(f'New tuple values: {new_tuple_values}')

            return {
                'message_type': message_type,
                'relation_id': relation_id,
                'old': old_tuple_values,
                'new': new_tuple_values,
                'diff': self.calculate_diff(old_tuple_values, new_tuple_values)
            }

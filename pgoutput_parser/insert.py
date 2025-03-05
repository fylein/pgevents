from common.log import get_logger

from .base import BaseMessage


logger = get_logger(__name__)


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
            new_tuple = self.read_utf_8(length=1)
            new_tuple_values = self.decode_tuple()

            return {
                'table_name': self.table_name,
                'new': new_tuple_values,
                'id': new_tuple_values['id'],
                'old': {},
                'diff': self.calculate_diff({}, new_tuple_values),
                'action': self.message_type,
                'recorded_at': self.recorded_at.isoformat()
            }

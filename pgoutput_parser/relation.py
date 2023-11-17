from common.log import get_logger
from common.utils import DeserializerUtils

from pgoutput_parser.base import BaseMessage


logger = get_logger(__name__)


class RelationMessage(BaseMessage):
    """Class for decoding PostgreSQL logical replication relation messages."""

    def decode_relation_message(self) -> dict:
        """
        Decode an relation message from the replication stream.

        :return: A dictionary containing the decoded relation message.
        """
        if self.message_type == 'R':
            logger.debug(f'Message: {self.message}')
            message_type = self.message_type

            relation_id = self.relation_id

            schema = self.read_string()
            table_name = f'{schema}.{self.read_string()}'

            # replica_identity
            self.read_int8()

            n_columns = self.read_int16()

            logger.debug(f'Message type: {message_type}')
            logger.debug(f'Relation ID: {relation_id}')
            logger.debug(f'Table name: {table_name}')
            logger.debug(f'Number of columns: {n_columns}')

            columns = []

            for _ in range(n_columns):
                # Flags
                self.read_int8()

                column_name = self.read_string()

                # Type ID
                self.read_int32()

                # Type modifier
                self.read_int32()
                
                logger.debug(f'Column name: {column_name}')

                columns.append({'name': column_name})
            
            return {
                'relation_id': relation_id,
                'table_name': table_name,
                'columns': columns
            }

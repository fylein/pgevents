import json
import logging
from common.event import PGEvent

logger = logging.getLogger(__name__)


class PGPlatformEvent(PGEvent):
    tablename: str = None
    action: str = None
    old: dict = None
    new: dict = None
    id: str = None
    updated_at: str = None
    updated_by: dict = None
    diff: dict = None

    def skip_col(self, tablename, colname):
        return False

    def clean_col_value(self, col):
        # pylint: disable=too-many-return-statements

        if col['name'] in ['updated_by', 'created_by']:
            logger.info('calling __clean_jsonb_str name: %s', col['name'])
            res = self.clean_jsonb_str(val=col['value'])
            return res

        return col['value']

    def loads(self, body):
        pl = json.loads(body)
        logger.debug('got payload %s', body)

        self.tablename = f"{pl['schema']}.{pl['table']}"
        self.action = pl['action']

        if self.action == 'I':
            self.new = self.clean_columns(self.tablename, pl['columns'])
            self.id = self.new.pop('id', None)
            self.updated_at = self.new.pop('updated_at', None)
            self.updated_by = self.new.pop('updated_by', None)
        elif self.action == 'U':
            self.new = self.clean_columns(self.tablename, pl['columns'])
            self.old = self.clean_columns(self.tablename, pl['identity'])
            self.id = self.new.pop('id', None)
            self.updated_at = self.new.pop('updated_at', None)
            self.updated_by = self.new.pop('updated_by', None)
            self.old.pop('updated_by', None)
            self.old.pop('updated_at', None)
            self.diff = self.diff_dict(self.old, self.new)
        elif self.action == 'D':
            self.old = self.clean_columns(self.tablename, pl['identity'])
            self.id = self.old.pop('id', None)
            self.updated_at = self.old.pop('updated_at', None)
            self.updated_by = self.old.pop('updated_by', None)
        else:
            raise NotImplementedError('for action: {0}'.format(self.action))

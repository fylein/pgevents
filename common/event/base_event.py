from json import JSONDecodeError
import json

from common.log import get_logger

logger = get_logger(__name__)


class BaseEvent:
    table_name: str = None
    old: dict = {}
    new: dict = {}
    id: str = None
    action: str = None
    diff: dict = {}

    @classmethod
    def process_text_array(cls, val):
        # pylint: disable=no-self-use
        if isinstance(val, str):
            if val.startswith('{') and val.endswith('}'):
                res = val[1:-1].split(',')
                return res

        return val

    @classmethod
    def process_json(cls, val):
        """
        this function recursively fixes jsonb that is encoded as string
        """
        if isinstance(val, str):
            str_val = val
            if str_val.startswith('['):
                str_list = []

                try:
                    str_list = json.loads(str_val)
                    res = []
                except JSONDecodeError as e:
                    logger.error("couldn't decode: %s, raise exception: %s", str_val, str(e))
                    res = None

                for str_v in str_list:
                    v = cls.process_json(str_v)
                    res.append(v)

                if res is None:
                    res = str_val

                return res

            elif str_val.startswith('{') and str_val.endswith('}'):
                try:
                    str_dict = json.loads(str_val)
                except JSONDecodeError as e:
                    logger.error("couldn't decode: %s, raise exception: %s", str_val, str(e))
                    str_dict = None

                if str_dict is None:
                    res = str_val
                else:
                    res = {}
                    for str_k, str_v in str_dict.items():
                        v = cls.process_json(str_v)
                        res[str_k] = v

                return res
            else:
                str_val = val
                res = str_val.replace('\\"', '"')

                return res
        else:
            return val

    @classmethod
    def clean_columns(cls, table_name, cols):
        d = {}

        if cols is None:
            return d

        for col in cols:
            k = col['name']
            if not cls.skip_col(table_name=table_name, col_name=k):
                v = cls.clean_col_value(col=col)
                d[k] = v

        return d

    @classmethod
    def diff_dict(cls, old_d, new_d):
        d = {}
        for nk, nv in new_d.items():
            if nk not in old_d and nv is not None:
                d[nk] = nv
            if nk in old_d and nv != old_d[nk]:
                d[nk] = nv
        return d

    @classmethod
    def skip_col(cls, table_name, col_name):
        return False

    @classmethod
    def clean_col_value(cls, col):
        # pylint: disable=too-many-return-statements

        if col['type'] in ['jsonb']:
            return cls.process_json(val=col['value'])

        if col['type'] in ['text[]']:
            res = cls.process_text_array(val=col['value'])
            return res

        return col['value']

    def load_wal2json_payload(self, body):
        pl = json.loads(body)
        logger.debug('got payload %s', body)

        self.table_name = f"{pl['schema']}.{pl['table']}"
        self.action = pl['action']

        if self.action == 'I':
            self.new = self.clean_columns(self.table_name, pl['columns'])
            self.id = self.new.get('id', None)
            self.diff = self.diff_dict(self.old, self.new)

        elif self.action == 'U':
            self.new = self.clean_columns(self.table_name, pl['columns'])
            self.old = self.clean_columns(self.table_name, pl['identity'])
            self.id = self.new.get('id', None)
            self.diff = self.diff_dict(self.old, self.new)

        elif self.action == 'D':
            self.old = self.clean_columns(self.table_name, pl['identity'])
            self.id = self.old.get('id', None)

        else:
            raise NotImplementedError()

    def __repr__(self):
        return "Event <table_name: %s>" % self.table_name

    def from_dict(self, payload_dict):
        self.table_name = payload_dict.get('table_name')
        self.old = payload_dict.get('old')
        self.new = payload_dict.get('new')
        self.id = payload_dict.get('id')
        self.diff = payload_dict.get('diff')
        self.action = payload_dict.get('action')

    def to_dict(self):
        return {
            'table_name': self.table_name,
            'old': self.old,
            'new': self.new,
            'id': self.id,
            'diff': self.diff,
            'action': self.action
        }

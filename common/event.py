import json
from json import JSONDecodeError
from abc import ABCMeta, abstractmethod

class PGEvent(metaclass=ABCMeta):
    @abstractmethod
    def loads(self, body):
        pass

    def clean_jsonb_str(self, val):
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
                    v = self.clean_jsonb_str(str_v)
                    res.append(v)

                if res is None:
                    res = str_val

                return res

            elif str_val.startswith('{'):
                res = {}
                str_dict = json.loads(str_val)

                for str_k, str_v in str_dict.items():
                    v = self.clean_jsonb_str(str_v)
                    res[str_k] = v

                return res
            else:
                str_val = val
                res = str_val.replace('\\"', '"')

                return res
        else:
            return val

    def clean_columns(self, tablename, cols):
        d = {}

        if cols is None:
            return d

        for col in cols:
            k = col['name']
            if not self.skip_col(tablename=tablename, colname=k):
                v = self.clean_col_value(col=col)
                d[k] = v

        return d

    def diff_dict(self, oldd, newd):
        d = {}
        for nk, nv in newd.items():
            if nk not in oldd and nv is not None:
                d[nk] = nv
            if nk in oldd and nv != oldd[nk]:
                d[nk] = nv
        return d

    def skip_col(self, tablename, colname):
        return False

    def clean_col_value(self, col):
        return col['value']

    def to_dict(self):
        return self.__dict__

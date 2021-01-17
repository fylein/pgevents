import json
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# this function recursively fixes jsonb that is encoded as string
def __clean_jsonb_str(val):
    if isinstance(val, str):
        strval = val
        if strval.startswith('['):
            res = []
            strlist = json.loads(strval)
            for strv in strlist:
                v = __clean_jsonb_str(strv)
                res.append(v)
            return res
        elif strval.startswith('{'):
            res = {}
            strdict = json.loads(strval)
            for strk, strv in strdict.items():
                v = __clean_jsonb_str(strv)
                res[strk] = v
            return res
        else:
            strval = val
            res = strval.replace('\\"', '"')
            return res
    else:
        return val


def __clean_col_value(col):
    # pylint: disable=too-many-return-statements

    if col['value'] is None:
        return None

    if col['type'] in ['location']:
        return col['value'].replace('\\"', '"')

    if col['type'] in ['jsonb']:
        return __clean_jsonb_str(val=col['value'])

    if col['name'] in ['extracted_data', 'custom_attributes', 'activity_details']:
        return __clean_jsonb_str(val=col['value'])

    if col['name'] in ['last_updated_by']:
        res = __clean_jsonb_str(val=col['value'])
        if isinstance(res, str):
            return {'org_user_id': res}
        return res

    return col['value']


def __skip_col(tablename, colname):
    # hack to ignore text_column1 etc columns - it is going to go away soon
    if 'public.transactions' in tablename:
        if '_column' in colname:
            return True
        if '_old' in colname:
            return True
        if 'custom_attributes' in colname:
            return True

    if 'public.users' in tablename:
        if 'password' in colname:
            return True
        if 'email' in colname:
            return True

    return False


def __clean_columns(tablename, cols):
    d = {}
    if cols is None:
        return d
    for col in cols:
        k = col['name']
        if not __skip_col(tablename=tablename, colname=k):
            v = __clean_col_value(col=col)
            d[k] = v
    return d


def __diff_dict(oldd, newd):
    d = {}
    for nk, nv in newd.items():
        if nk not in oldd and nv is not None:
            d[nk] = nv
        if nk in oldd and nv != oldd[nk]:
            d[nk] = nv
    return d


@dataclass
class Event:
    tablename = None
    action = None
    old = None
    new = None
    id = None
    updated_at = None
    updated_by = None
    diff = None

    def to_dict(self):
        logger.info("aaaaaaaaddddddddddiiiiiiiiiiii")
        return self.__dict__


def msg_to_event(pgdatabase, msg):
    pl = json.loads(msg.payload)
    if pl['action'] not in ['I', 'U', 'D']:
        return None
    logger.debug('got payload %s', msg.payload)

    event = Event()

    event.tablename = f"{pgdatabase}.{pl['schema']}.{pl['table']}"
    event.action = pl['action']

    if event.action == 'I':
        event.new = __clean_columns(event.tablename, pl['columns'])
        event.id = event.new.pop('id', None)
        event.updated_at = event.new.pop('updated_at', None)
        event.updated_by = event.new.pop('last_updated_by', None)
    elif event.action == 'U':
        event.new = __clean_columns(event.tablename, pl['columns'])
        event.old = __clean_columns(event.tablename, pl['identity'])
        event.id = event.new.pop('id', None)
        event.updated_at = event.new.pop('updated_at', None)
        event.updated_by = event.new.pop('last_updated_by', None)
        event.old.pop('last_updated_by', None)
        event.old.pop('updated_at', None)
        event.diff = __diff_dict(event.old, event.new)
    elif event.action == 'D':
        event.old = __clean_columns(event.tablename, pl['identity'])
        event.id = event.old.pop('id', None)
        event.updated_at = event.old.pop('updated_at', None)
        event.updated_by = event.old.pop('last_updated_by', None)

    return event

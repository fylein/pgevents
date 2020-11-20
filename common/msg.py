import os
import json
import logging
import copy

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
    if col['value'] is None:
        return None

    if col['type'] in ['location', 'jsonb']:
        return col['value'].replace('\\"', '"')

    if col['name'] in ['last_updated_by', 'extracted_data', 'custom_attributes', 'activity_details']:
        return __clean_jsonb_str(val=col['value'])

    return col['value']

def __clean_columns(cols):
    d = {}
    if cols is None:
        return d
    for col in cols:
        k = col['name']
        v = __clean_col_value(col=col)
        d[k] = v
    return d

def __diff_dict(oldd, newd):
    d = {}
    for nk, nv in newd.items():
        if nk not in oldd and nv is not None:
            d[k] = nv
        if nk in oldd and nv != oldd[nk]:
            d[k] = nv
    return d

def __find_value(action, oldd, newd, colname):
    if action == 'D':
        if colname in oldd:
            return oldd[colname]
    else:
        if colname in newd:
            return newd[colname]
    return None

def __msg_to_event(pgdatabase, msg):
    logger.debug('got payload %s', msg.payload)
    pl = json.loads(msg.payload)
    if pl['action'] not in ['I', 'U', 'D']:
        return None

    event = {
        'tablename': None,
        'action': None,
        'old': None,
        'new': None,
        'id': None,
        'updated_at': None,
        'updated_by': None,
        'diff': None
    }

    event['tablename'] = f"{pgdatabase}.{pl['schema']}.{pl['table']}"
    event['action'] = pl['action']

    if event['action'] == 'I':
        event['new'] = __clean_columns(pl['columns'])
        event['id'] = event['new'].pop('id', None)
        event['updated_at'] = event['new'].pop('updated_at', None)
        event['updated_by'] = event['new'].pop('last_updated_by', None)
    elif event['action'] == 'U':
        event['new'] = __clean_columns(pl['columns'])
        event['old'] = __clean_columns(pl['identity'])
        event['diff'] = __diff_dict(event['old'], event['new'])
        event['id'] = event['new'].pop('id', None)
        event['updated_at'] = event['new'].pop('updated_at', None)
        event['updated_by'] = event['new'].pop('last_updated_by', None)
    elif event['action'] == 'D':
        event['old'] = __clean_columns(pl['identity'])
        event['id'] = event['old'].pop('id', None)
        event['updated_at'] = event['old'].pop('updated_at', None)
        event['updated_by'] = event['old'].pop('last_updated_by', None)

    return event

def consume_stream(pgdatabase, msg, process_event_fn):
    event = __msg_to_event(pgdatabase, msg)
    if event:
        process_event_fn(event=event)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

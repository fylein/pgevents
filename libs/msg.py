import os
import json
import logging
import copy

logger = logging.getLogger(__name__)

# events as the following structure
# action
# new
# old
# routing_key
# pk
def __msg_to_event(msg):
    logger.debug('got payload %s', msg.payload)
    event = json.loads(msg.payload)
    if event['action'] == 'I':
        event['new'] = event['columns']
        event['routing_key'] = f"{event['schema']}.{event['table']}"
        del event['columns']
    elif event['action'] == 'U':
        event['new'] = event['columns']
        event['old'] = event['identity']
        event['routing_key'] = f"{event['schema']}.{event['table']}"
        del event['columns']
        del event['identity']
    elif event['action'] == 'D':
        event['old'] = event['identity']
        event['routing_key'] = f"{event['schema']}.{event['table']}"
        del event['identity']
    else:
        event = None
    return event


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


def __clean_column(col):
    if col['type'] == 'location' and col['value'] is not None:
        col['value'] = __clean_jsonb_str(val=col['value'])

    if col['type'] == 'jsonb' and col['value'] is not None:
        col['value'] = __clean_jsonb_str(val=col['value'])

    if col['name'] in ['last_updated_by', 'extracted_data', 'custom_attributes', 'activity_details'] and col['type'] == 'text' and col['value'] is not None:
        col['value'] = __clean_jsonb_str(val=col['value'])
        col['type'] = 'jsonb'

    return col

def __clean_columns(cols):
    if cols is None:
        return None
    res = []
    for col in cols:
        col = __clean_column(col=col)
        res.append(col)
    return res

def __diff(oldc, newc):
    if oldc is None or newc is None:
        return None
    d = []
    for col in newc:
        if col not in oldc and col['value'] is not None:
            d.append(col)
    return d

def __clean_event(event):
    event['old'] = __clean_columns(cols=event['old'])
    event['new'] = __clean_columns(cols=event['new'])
    event['diff'] = __diff(event['old'], event['new'])
    return event

def consume_stream(msg, process_event_fn):
    event = __msg_to_event(msg)
    if event:
        event = __clean_event(event)
        process_event_fn(event=event)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

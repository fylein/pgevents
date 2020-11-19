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

def __clean_columns(cols):
    if cols is None:
        return None
    for c in cols:
        if c['type'] == 'jsonb' and c['value'] is not None:
            c['value'] = json.loads(c['value'].replace('\\"', '"'))
        if c['type'] == 'location' and c['value'] is not None:
            c['value'] = c['value'].replace('\\"', '"')
    return cols

def __clean_event(event):
    event['old'] = __clean_columns(cols=event['old'])
    event['new'] = __clean_columns(cols=event['new'])
    return event

def consume_stream(msg, process_event_fn):
    event = __msg_to_event(msg)
    if event:
        event = __clean_event(event)
        process_event_fn(event=event)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

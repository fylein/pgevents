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

def consume_stream(msg, process_event_fn):
    event = __msg_to_event(msg)
    if event:
        process_event_fn(event=event)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

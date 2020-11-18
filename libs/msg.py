import os
import json
import logging
import copy
import re

logger = logging.getLogger(__name__)

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

def consume_stream(msg, whitelist_regex_c, blacklist_regex_c, process_event_fn):
    event = __msg_to_event(msg)
    allowed = (event is not None)
    if allowed and whitelist_regex_c:
        if not re.match(whitelist_regex_c, event['routing_key']):
            logger.debug('did not pass whitelist %s', event['routing_key'])
            allowed = False
    if allowed and blacklist_regex_c:
        if re.match(blacklist_regex_c, event['routing_key']):
            logger.debug('matched blacklist %s', event['routing_key'])
            allowed = False
    if allowed:
        process_event_fn(event=event)
    else:
        logger.debug('skipping event %s', event)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

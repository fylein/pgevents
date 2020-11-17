import os
import json
import logging

logger = logging.getLogger(__name__)

def __insert_event(change):
    event = {
        'kind': 'insert',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['columnnames'], change['columntypes'])),
        'old': None,
        'new': dict(zip(change['columnnames'], change['columnvalues']))
    }
    return event

def __update_event(change):
    event = {
        'kind': 'update',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['columnnames'], change['columntypes'])),
        'old': dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues'])),
        'new': dict(zip(change['columnnames'], change['columnvalues']))
    }
    return event

def __delete_event(change):
    event = {
        'kind': 'delete',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keytypes'])),
        'old': dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keyvalues'])),
        'new': None
    }
    return event

def __msg_to_events_generator(msg):
    obj = json.loads(msg.payload)
    if 'change' in obj:
        for change in obj['change']:
            event = None
            if change['kind'] == 'insert':
                event = __insert_event(change)
            elif change['kind'] == 'update':
                event = __update_event(change)
            elif change['kind'] == 'delete':
                event = __delete_event(change)
            if event:
                event['sent_at'] = msg.send_time
                event['lsn'] = msg.data_start
                yield event

def consume_stream(msg, whitelist_regex_c, blacklist_regex_c, process_event_fn):
    logger.debug('got payload %s', msg.payload)
    for event in __msg_to_events_generator(msg=msg):
        allowed = True
        if whitelist_regex_c:
            if not re.match(whitelist_regex_c, event['table']):
                logger.debug('did not pass whitelist %s', event['table'])
                allowed = False
        if allowed and blacklist_regex_c:
            if re.match(blacklist_regex_c, event['table']):
                logger.debug('matched blacklist %s', event['table'])
                allowed = False
        if allowed:
            process_event_fn(event=event)
        else:
            logger.debug('skipping event %s', event)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)

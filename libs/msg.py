import os
import json
import logging

logger = logging.getLogger(__name__)

def __clean_values(types, values):
    ret_values = []
    for i, t in enumerate(types):
        if t in ['jsonb', 'location', 'json']:
            ret_values[i] = values[i].replace('\\"', '"')
        else:
            ret_values[i] = values[i]
    return ret_values

def __insert_event(change):
    new_values = __clean_values(change['columntypes'], change['columnvalues'])
    event = {
        'kind': 'insert',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['columnnames'], change['columntypes'])),
        'old': None,
        'new': dict(zip(change['columnnames'], new_values))
    }
#    event['diff'] = event['new']
    return event

def __update_event(change):
    old_values = __clean_values(change['columntypes'], change['oldkeys']['keyvalues']))
    new_values = __clean_values(change['columntypes'], change['columnvalues'])
    event = {
        'kind': 'update',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['columnnames'], change['columntypes'])),
        'old': dict(zip(change['oldkeys']['keynames'], old_values)),
        'new': dict(zip(change['columnnames'], new_values))
    }
#    event['diff'] = None
    return event

def __delete_event(change):
    old_values = __clean_values(change['columntypes'], change['oldkeys']['keyvalues']))
    event = {
        'kind': 'delete',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['oldkeys']['keynames'], change['oldkeys']['keytypes'])),
        'old': dict(zip(change['oldkeys']['keynames'], old_values)),
        'new': None
    }
#    event['diff'] = event['old']
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

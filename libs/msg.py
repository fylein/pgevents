import os
import json
import logging
import re

logger = logging.getLogger(__name__)

def __diff(oldv, newv):
    logger.debug('calling diff on %s, %s', oldv, newv)
    if oldv is None:
        return newv
    if newv is None:
        return oldv
    d = {}
    for k in oldv.keys():
        ov = oldv[k]
        if k in newv:
            nv = newv[k]
            if nv != ov:
                if isinstance(nv, dict):
                    d[k] = __diff(ov, nv)
                else:
                    d[k] = nv
            elif k == 'id':
                d[k] = nv
        else:
            if ov is not None:
                d[k] = None
    for k in newv.keys():
        if k not in oldv:
            nv = newv[k]
            if nv is not None:
                d[k] = nv
    return d

def __clean_values(types, values):
    ret_values = []
    for i, t in enumerate(types):
        if values[i] is None:
            ret_values.append(values[i])
        else:
            if t in ['jsonb']:
                v = json.loads(values[i])
                ret_values.append(v)
            elif t in ['location']:
                ret_values.append(values[i].replace('\\"', '"'))
            else:
                ret_values.append(values[i])
    return ret_values

def __insert_event(change):
#    new_values = __clean_values(change['columntypes'], change['columnvalues'])
    new_values = change['columnvalues']
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
    # old_values = __clean_values(change['oldkeys']['keytypes'], change['oldkeys']['keyvalues'])
    # new_values = __clean_values(change['columntypes'], change['columnvalues'])

    old_values = change['oldkeys']['keyvalues']
    new_values = change['columnvalues']
    event = {
        'kind': 'update',
        'table': '{0}.{1}'.format(change['schema'], change['table']),
        'types': dict(zip(change['columnnames'], change['columntypes'])),
        'old': dict(zip(change['oldkeys']['keynames'], old_values)),
        'new': dict(zip(change['columnnames'], new_values))
    }
#    event['diff'] = __diff(event['old'], event['new'])
    return event

def __delete_event(change):
#    old_values = __clean_values(change['oldkeys']['keytypes'], change['oldkeys']['keyvalues'])
    old_values = change['oldkeys']['keyvalues']
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
            logger.debug('processing change %s', change)
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

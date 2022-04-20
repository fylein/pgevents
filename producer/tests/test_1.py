import json
from time import sleep
from src.common import log


logger = log.get_logger(__name__)


class TestProducerEvent:

    def test_insert(self, db_conn, rmq_conn):
        with db_conn.cursor() as db_cursor:
            db_cursor.execute(
                '''
                    insert into users(id, full_name, dob, email, is_email_verified)
                    values (%s,%s,%s,%s,%s)
                ''', (
                    'user1', 'user_fullname1', '1995-02-02', 'user1@someorg.in', False
                )
            )
        db_conn.commit()
        sleep(1)

        events = rmq_conn.consume_all()

        assert len(events) == 1

        routing_key, event_body = events[0]
        event = json.loads(event_body)

        assert routing_key == 'public.users'
        assert event['table_name'] == 'public.users'
        assert event['action'] == 'I'
        assert event['old'] == {}
        assert event['id'] == 'user1'
        assert event['new']['id'] == 'user1'
        assert event['new']['full_name'] == 'user_fullname1'
        assert event['new']['email'] == 'user1@someorg.in'
        assert event['new']['is_email_verified'] is False
        assert event['new']['dob'] == '1995-02-02 00:00:00+00'

    def test_update(self, db_conn, rmq_conn):
        with db_conn.cursor() as db_cursor:
            db_cursor.execute(
                '''
                    update users set is_email_verified=%s, full_name=%s
                    where id=%s
                ''', (
                    True, 'user_fullname_updated1', 'user1'
                )
            )
        db_conn.commit()
        sleep(1)

        events = rmq_conn.consume_all()

        assert len(events) == 1

        routing_key, event_body = events[0]
        event = json.loads(event_body)

        assert routing_key == 'public.users'
        assert event['table_name'] == 'public.users'
        assert event['action'] == 'U'

        assert event['old']['full_name'] == 'user_fullname1'
        assert event['old']['email'] == 'user1@someorg.in'
        assert event['old']['is_email_verified'] is False
        assert event['old']['dob'] == '1995-02-02 00:00:00+00'

        assert event['diff']['is_email_verified'] is True
        assert event['diff']['full_name'] == 'user_fullname_updated1'

        assert event['new']['full_name'] == 'user_fullname_updated1'
        assert event['new']['email'] == 'user1@someorg.in'
        assert event['new']['is_email_verified'] is True
        assert event['new']['dob'] == '1995-02-02 00:00:00+00'
# import json
# from time import sleep
# from common import log


# logger = log.get_logger(__name__)


# class TestEventProducer:

#     def test_insert(self, db_conn, rmq_conn):
#         with db_conn.cursor() as db_cursor:
#             db_cursor.execute(
#                 '''
#                     insert into users(id, full_name)
#                     values (%s,%s)
#                 ''', (
#                     2, 'Tony Iommi'
#                 )
#             )
#         db_conn.commit()
#         sleep(1)

#         events = rmq_conn.consume_all()

#         assert len(events) == 1

#         routing_key, event_body = events[0]
#         event = json.loads(event_body)

#         assert routing_key == 'public.users'
#         assert event['table_name'] == 'public.users'
#         assert event['action'] == 'I'
#         assert event['old'] == {}
#         assert event['id'] == 2
#         assert event['new']['id'] == 2
#         assert event['new']['full_name'] == 'Tony Iommi'

#     def test_update(self, db_conn, rmq_conn):
#         with db_conn.cursor() as db_cursor:
#             db_cursor.execute(
#                 '''
#                     update users set full_name=%s
#                     where id=%s
#                 ''', (
#                     'Geezer Butler', '2'
#                 )
#             )
#         db_conn.commit()
#         sleep(1)

#         events = rmq_conn.consume_all()

#         assert len(events) == 1

#         routing_key, event_body = events[0]
#         event = json.loads(event_body)

#         assert routing_key == 'public.users'
#         assert event['table_name'] == 'public.users'
#         assert event['action'] == 'U'
#         assert event['old']['full_name'] == 'Tony Iommi'
#         assert event['diff']['full_name'] == 'Geezer Butler'
#         assert event['new']['full_name'] == 'Geezer Butler'

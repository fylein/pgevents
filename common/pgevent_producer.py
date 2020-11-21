import json
import logging

import pika
import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection

from common.compression import compress
from common.decorators import retry
from common.msg import msg_to_event

logger = logging.getLogger(__name__)

class PGEventProducerShutdownException(Exception):
    pass

class PGEventProducer:
    def __init__(self, pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, pgtables, rabbitmq_url, rabbitmq_exchange):
        self.__pghost = pghost
        self.__pgport = pgport
        self.__pgdatabase = pgdatabase
        self.__pguser = pguser
        self.__pgpassword = pgpassword
        self.__pgslot = pgslot
        self.__pgtables = pgtables
        self.__rabbitmq_url = rabbitmq_url
        self.__rabbitmq_exchange = rabbitmq_exchange
        self.__db_conn = None
        self.__db_cur = None
        self.__rmq_conn = None
        self.__rmq_channel = None
        self.__shutdown = False

    @retry(n=3, backoff=15, exceptions=(psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.ProgrammingError))
    def __connect_db(self):
        self.__check_shutdown()
        self.__db_conn = psycopg2.connect(host=self.__pghost, port=self.__pgport, dbname=self.__pgdatabase, user=self.__pguser,
                                          password=self.__pgpassword, connection_factory=LogicalReplicationConnection)
        self.__db_cur = self.__db_conn.cursor()
        options = {'format-version': 2, 'include-types': True, 'include-lsn': True}
        if self.__pgtables and len(self.__pgtables) > 0:
            options['add-tables'] = self.__pgtables
        logger.debug('options for slot %s', options)
        self.__db_cur.start_replication(slot_name=self.__pgslot, options=options, decode=True)
        logger.debug('started consuming')

    @retry(n=3, backoff=15, exceptions=(pika.exceptions.StreamLostError, pika.exceptions.AMQPConnectionError))
    def __connect_rabbitmq(self):
        self.__check_shutdown()
        self.__rmq_conn = pika.BlockingConnection(pika.URLParameters(self.__rabbitmq_url))
        self.__rmq_channel = self.__rmq_conn.channel()
        self.__rmq_channel.exchange_declare(exchange=self.__rabbitmq_exchange, exchange_type='topic')

    def __check_shutdown(self):
        if self.__shutdown:
            raise PGEventProducerShutdownException('shutting down')

    def __send_event(self, event):
        routing_key = event['tablename']
        body = json.dumps(event, sort_keys=True, default=str)
        bodyc = compress(body)
        # bodyd = decompress(bodyc)
        # assert body == bodyd, 'compression is losing data'
        # logger.debug('original body size %s, compressed size %s, savings %s', len(body), len(bodyc), len(body) * 1.0 / len(bodyc))
        logger.debug('sending routing_key %s bodyc bytes %s ', routing_key, len(bodyc))
        self.__rmq_channel.basic_publish(exchange=self.__rabbitmq_exchange, routing_key=routing_key, body=bodyc)

    def __consume_stream(self, msg):
        self.__check_shutdown()
        event = msg_to_event(self.__pgdatabase, msg)
        if event:
            self.__send_event(event=event)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def shutdown(self, *args):
        #pylint: disable=unused-argument
        logger.warning('Shutdown has been requested')
        self.__shutdown = True

    def process(self):
        try:
            self.__connect_db()
            self.__connect_rabbitmq()
            logger.info('connected to db and rabbitmq successfully')
            self.__db_cur.consume_stream(self.__consume_stream)
        except PGEventProducerShutdownException:
            logger.warning('exiting process loop')
            return

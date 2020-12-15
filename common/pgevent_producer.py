import json
import logging

# import pika
import psycopg2
import psycopg2.errorcodes
from psycopg2.extras import LogicalReplicationConnection

from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN

from common.compression import compress
from common.decorators import retry
from common.msg import msg_to_event

logger = logging.getLogger(__name__)

HOSTNAME = os.environ.get('HOSTNAME', 'pgevent_producer')


class PGEventProducerShutdownException(Exception):
    pass


class PGEventProducer:
    def __init__(self, pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, pgtables, nats_url, nats_cluster_name):
        self.__pghost = pghost
        self.__pgport = pgport
        self.__pgdatabase = pgdatabase
        self.__pguser = pguser
        self.__pgpassword = pgpassword
        self.__pgslot = pgslot
        self.__pgtables = pgtables
        self.__nats_url = nats_url
        self.__nats_cluster_name = nats_cluster_name
        self.__db_conn = None
        self.__db_cur = None
        self.__nats_conn = None
        self.__shutdown = False

    @retry(n=3, backoff=15, exceptions=(psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.ProgrammingError))
    def __connect_db(self):
        self.__check_shutdown()
        self.__db_conn = psycopg2.connect(host=self.__pghost, port=self.__pgport, dbname=self.__pgdatabase, user=self.__pguser,
                                          password=self.__pgpassword, connection_factory=LogicalReplicationConnection)
        self.__db_cur = self.__db_conn.cursor()
        options = {'format-version': 2,
                   'include-types': True, 'include-lsn': True}
        if self.__pgtables and len(self.__pgtables) > 0:
            options['add-tables'] = self.__pgtables
        logger.debug('options for slot %s', options)
        self.__db_cur.start_replication(
            slot_name=self.__pgslot, options=options, decode=True)
        logger.debug('started consuming')

    @retry(n=1, backoff=15, exceptions=())
    def __connect_nats(self):
        self.__check_shutdown()
        nc = NATS()
        sc = STAN()
        nc.connect(self.__nats_url)
        sc.connect("test-cluster", HOSTNAME, nats=nc)
        self.__nats_conn = (
            nc, sc
        )

    def __check_shutdown(self):
        if self.__shutdown:
            raise PGEventProducerShutdownException('shutting down')

    def ack_handler(ack):
        logger.info("Received ack: {}".format(ack.guid))
        pass

    def __send_event(self, event):
        routing_key = event['tablename']
        body = json.dumps(event, sort_keys=True, default=str)
        bodyc = compress(body)
        logger.debug('sending routing_key %s bodyc bytes %s ',
                     routing_key, len(bodyc))
        self.__nats_conn[1].publish(
            routing_key, bodyc, ack_handler=ack_handler)

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
            self.__connect_nats()
            logger.debug('connected to db and nats successfully')
            self.__db_cur.consume_stream(self.__consume_stream)
        except PGEventProducerShutdownException:
            logger.warning('exiting process loop')
            return

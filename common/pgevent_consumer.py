import json
import logging

import pika

from common.decorators import retry
from common.compression import decompress

logger = logging.getLogger(__name__)


class PGEventConsumerShutdownException(Exception):
    pass


class PGEventConsumer:
    def __init__(self, queue_name, binding_keys, process_event_fn, nats_url, nats_cluster_name):
        self.__nats_url = nats_url
        self.__nats_cluster_name = nats_cluster_name
        self.__queue_name = queue_name
        self.__binding_keys = binding_keys
        self.__shutdown = False
        self.__process_event_fn = process_event_fn
        self.__nats_conn = None
        self.__nats_sub = None

    def cb(msg):
        # nonlocal sc
        print("Received a message on subscription (seq: {}): {}".format(
            msg.sequence, msg.data))

        # this can be an async task if we wanted to use all resources
        # asyncio.create_task(process_event(msg))
        # await process_event(msg)

        process_event(msg)

        # ack after the processing is complete
        # to discuss ? this helps in retry and same as eh today
        await self.__nats_conn[1].ack(msg)

        print("acknowledged for msg seq {}".format(msg.sequence))

    @retry(n=1, backoff=15, exceptions=())
    def __connect_nats(self):
        self.__check_shutdown()
        nc = NATS()
        sc = STAN()
        nc.connect(self.__nats_url)
        sc.connect(self.__nats_cluster_name, HOSTNAME, nats=nc)
        self.__nats_conn = (
            nc, sc
        )

        self.__nats_sub = sc.subscribe(
            "foo",  # topic/subject
            durable_name="bar",   # used for continuation
            queue="bar",          # used for grouping and queing
            cb=cb,                # the function to be caled on recieving of message
            max_inflight=1,       # max number of unacknowleged messages that nats-server will send
            manual_acks=True,     # flag to manually acknowledge
            # timeout time to wait for an acknowledment before retrying (this should be some high value)
            ack_wait=100,
            # deliver_all_available=True
        )

    def __check_shutdown(self):
        if self.__shutdown:
            raise PGEventConsumerShutdownException('shutting down')

    def __process_body(self, ch, method, properties, body):
        #pylint: disable=unused-argument
        self.__check_shutdown()
        bodyu = decompress(body)
        event = json.loads(bodyu)
        self.__process_event_fn(event)

    def process(self):
        try:
            self.__rmq_channel.basic_consume(
                queue=self.__queue_name, on_message_callback=self.__process_body, auto_ack=True)
        except PGEventConsumerShutdownException:
            logger.warning('exiting process loop')
            return

    def shutdown(self, *args):
        #pylint: disable=unused-argument
        logger.warning('Shutdown has been requested')
        self.__shutdown = True

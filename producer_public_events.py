import logging
import os
import signal
import json
import click

from common.logging import init_logging
from common.pgevent_producer import PGEventProducer

logger = logging.getLogger(__name__)


def convert_pgevent_to_public_event(pgdatabase, event):
    routing_key = None
    payload = None
    body = None

    logger.debug("convert_pgevent_to_public_event for table: %s and action: %s", event.tablename, event.action)

    logger.debug("event updated_by: %s", event.updated_by)

    if event.updated_by and event.updated_by.get('written_from') != 'platform-api':
        logger.debug('skipping this event, was not written from platform-api')
        return routing_key, body

    if event.tablename == f"{pgdatabase}.platform_schema.employees_rot":
        if event.action == 'I':
            routing_key = 'eous.created'
            payload = {
                'id': event.id,
                'body': {
                    'inviterId': event.updated_by.get('employee_id')
                }
            }
        elif event.action == 'U':
            routing_key = 'eous.policy_run'
            payload = {
                'id': event.id
            }

    if payload is not None:
        body = json.dumps(payload)

    return routing_key, body


class PGEventProducerPublicEvent(PGEventProducer):
    def intercept(self, pgdatabase, event):
        return convert_pgevent_to_public_event(pgdatabase, event)


@click.command()
@click.option('--pghost', default=lambda: os.environ.get('PGHOST', None), required=True, help='Postgresql Host ($PGHOST)')
@click.option('--pgport', default=lambda: os.environ.get('PGPORT', 5432), required=True, help='Postgresql Host ($PGPORT)')
@click.option('--pgdatabase', default=lambda: os.environ.get('PGDATABASE', None), required=True, help='Postgresql Database ($PGDATABASE)')
@click.option('--pguser', default=lambda: os.environ.get('PGUSER', None), required=True, help='Postgresql User ($PGUSER)')
@click.option('--pgpassword', default=lambda: os.environ.get('PGPASSWORD', None), required=True, help='Postgresql Password ($PGPASSWORD)')
@click.option('--pgslot', default=lambda: os.environ.get('PGSLOT', None), required=True, help='Postgresql Replication Slot Name ($PGSLOT)')
@click.option('--pgtables', default=lambda: os.environ.get('PGTABLES', None), required=False, help='Restrict to specific tables e.g. public.transactions,public.reports')
@click.option('--rabbitmq-url', default=lambda: os.environ.get('RABBITMQ_URL', None), required=True, help='RabbitMQ url ($RABBITMQ_URL)')
@click.option('--rabbitmq-exchange', default=lambda: os.environ.get('RABBITMQ_EXCHANGE', None), required=True, help='RabbitMQ exchange ($RABBITMQ_EXCHANGE)')
def producer(pghost, pgport, pgdatabase, pguser, pgpassword, pgslot, pgtables, rabbitmq_url, rabbitmq_exchange):
    init_logging()
    p = PGEventProducerPublicEvent(pghost=pghost, pgport=pgport, pgdatabase=pgdatabase, pguser=pguser, pgpassword=pgpassword,
                                   pgslot=pgslot, pgtables=pgtables, rabbitmq_url=rabbitmq_url, rabbitmq_exchange=rabbitmq_exchange)
    signal.signal(signal.SIGTERM, p.shutdown)
    signal.signal(signal.SIGINT, p.shutdown)
    p.process()


if __name__ == '__main__':
    producer()

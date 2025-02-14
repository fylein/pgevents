import os
import logging

import simplejson

import datetime
from datetime import datetime as dt
from dateutil import parser
import time


logger = logging.getLogger(__name__)


class FyleJsonDecodeError(Exception):
    pass


class FyleJsonEncoder(simplejson.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
            # Note: datetime is handled by marshmallow is schema.dump is used
            return o.isoformat()

        return super().default(o)


def dumps(d, **kwargs):
    if kwargs.get('cls') is None:
        kwargs['cls'] = FyleJsonEncoder

    return simplejson.dumps(d, **kwargs)


def get_utc_now() -> datetime:
    return dt.now(datetime.timezone.utc)


def iso_parse_date_str(*, date_str) -> datetime:
    date_time = parser.isoparse(date_str)

    return date_time


class RetryException(Exception):
    pass


def retry(*, n, backoff, exceptions) -> object:
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a retry attempt
    :type Exceptions: Tuple of Exceptions
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 1
            while attempt <= n:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    logger.exception(
                        'Exception thrown when attempting to run %s, attempt '
                        '%d of %d, retrying after %s seconds', func, attempt, n, backoff
                    )
                    time.sleep(backoff)
                    attempt += 1
            raise RetryException('failed to execute %s despite retrying' % (func))
        return newfn
    return decorator

import logging
import time

logger = logging.getLogger(__name__)

class RetryException(Exception):
    pass

def retry(n, backoff, exceptions):
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
            attempt = 0
            while attempt < n:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.exception(
                        'Exception thrown when attempting to run %s, attempt '
                        '%d of %d, retrying after %s seconds' % (func, attempt, n, backoff)
                    )
                    time.sleep(backoff)
                    attempt += 1
            raise RetryException('failed to execute %s despite retrying')
        return newfn
    return decorator
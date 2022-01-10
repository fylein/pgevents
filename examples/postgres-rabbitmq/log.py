import logging
import os

LOG_LEVEL = os.environ.get('LOG_LEVEL')


def get_logger(name):
    logger = logging.getLogger(name)
    logger.level = logging.__dict__[LOG_LEVEL]
    handler = logging.StreamHandler()

    logger.addHandler(handler)
    return logger

import logging
import logging.config
import yaml

def init_logging():
    with open('logging.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)


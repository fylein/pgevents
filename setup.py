from setuptools import setup, find_packages

setup(
    name='fyle-pgevents',
    version='0.1',
    packages=find_packages(
        include=['src', 'producer', 'consumer']
    ),
    entry_points='''
        [console_scripts]
        producer=producer.producer:producer
        event_logger=consumer.event_logger:log_event
    '''
)

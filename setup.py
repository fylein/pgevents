from setuptools import setup, find_packages

setup(
    name='pgevents',
    version='0.2',
    packages=find_packages(
        include=['src', 'producer', 'consumer']
    ),
    entry_points='''
        [console_scripts]
        producer=producer.main:produce
        producer_multiple_dbs=producer.main:producer_multiple_dbs
        event_logger=consumer.main:consume
    '''
)

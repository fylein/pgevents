from setuptools import setup, find_packages

setup(
    name='pgevents',
    version='0.1',
    packages=find_packages(
        include=['src', 'producer', 'consumer']
    ),
    entry_points='''
        [console_scripts]
        producer=producer.main:produce
        event_logger=consumer.main:consume
    '''
)

from setuptools import setup, find_packages

setup(
    name='pgevents',
    version='0.4',
    packages=find_packages(
        include=['src', 'producer', 'consumer', 'common*']
    ),
    install_requires=[
        'Brotli==1.1.0',
        'Click==8.1.8',
        'pika==1.3.2',
    ],
    entry_points='''
        [console_scripts]
        producer=producer.main:produce
        event_logger=consumer.main:consume
    '''
)

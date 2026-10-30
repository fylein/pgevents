from setuptools import setup, find_packages

setup(
    name='pgevents',
    version='0.5',
    packages=find_packages(
        include=['src', 'producer', 'consumer', 'common*', 'pgoutput_parser']
    ),
    license_files=['LICENSE'],
    install_requires=[
        'Brotli==1.2.1',
        'Click==8.3.3',
        'pika==1.3.2',
        'psycopg2==2.9.10',
    ],
    entry_points='''
        [console_scripts]
        producer=producer.main:produce
        event_logger=consumer.main:consume
    '''
)

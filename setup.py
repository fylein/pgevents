
from setuptools import setup

setup(
    name="pg_recvlogical",
    version='0.1',
    packages=['', 'common'],
    install_requires=[
        'pika==1.1.0',
        'psycopg2==2.8.6',
        'click==7.0'
    ],
    entry_points='''
        [console_scripts]
        pgevent_producer=pgevent_producer:pgevent_producer
        pgevent_consumer_debug=pgevent_consumer_debug:pgevent_consumer_debug
        pgevent_consumer_audit=pgevent_consumer_audit:pgevent_consumer_audit
    ''',
)
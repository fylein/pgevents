
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
        producer=producer:producer
        consumer_debug=consumer_debug:consumer_debug
        consumer_audit=consumer_audit:consumer_audit
    ''',
)
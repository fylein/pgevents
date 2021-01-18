
from setuptools import setup

setup(
    name="pg_recvlogical",
    version='0.1',
    packages=['', 'common'],
    install_requires=[
        'pika==1.1.0',
        'psycopg2-binary==2.8.6',
        'click==7.0',
        'brotli==1.0.9'
    ],
    entry_points='''
        [console_scripts]
        producer=producer:producer
        producer_public_events=producer_public_events:producer
        consumer_debug=consumer_debug:consumer_debug
        consumer_audit=consumer_audit:consumer_audit
    ''',
)


from setuptools import setup, find_packages

setup(
    name="pg_recvlogical",
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pika==1.1.0',
        'psycopg2==2.8.6',
        'click==7.0',
        'brotli==1.0.9'
    ],
    entry_points='''
        [console_scripts]
        producer=producers.pgevent_producer:producer
        consumer_debug=consumer_debug:consumer_debug
        consumer_audit_public=consumers.public.consumer_audit:consumer_audit
        consumer_audit_platform=consumers.platform.consumer_audit:consumer_audit
        consumer_event_bridge=consumers.platform.consumer_event_bridge:consumer_event_bridge
    ''',
)

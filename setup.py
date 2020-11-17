
from setuptools import setup

setup(
    name="pg_recvlogical",
    version='0.1',
    py_modules=['pg_recvlogical'],
    install_requires=[
        'pika==1.1.0',
        'psycopg2==2.8.6',
        'click==7.0'
    ],
    entry_points='''
        [console_scripts]
        stdout_writer=pg_recvlogical:stdout_writer
        rabbitmq_writer=pg_recvlogical:rabbitmq_writer
        rabbitmq_reader=pg_recvlogical:rabbitmq_reader
    ''',
)
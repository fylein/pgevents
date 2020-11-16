
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
        stdout=pg_recvlogical:stdout
        rabbitmq=pg_recvlogical:rabbitmq
    ''',
)

from setuptools import setup

setup(
    name="pg_recvlogical",
    version='0.1',
    packages=['', 'libs'],
    install_requires=[
        'pika==1.1.0',
        'psycopg2==2.8.6',
        'click==7.0'
    ],
    entry_points='''
        [console_scripts]
        pg_to_rabbitmq=pg_to_rabbitmq:pg_to_rabbitmq
        pg_to_stdout=pg_to_stdout:pg_to_stdout
        rabbitmq_to_stdout=rabbitmq_to_stdout:rabbitmq_to_stdout
    ''',
)
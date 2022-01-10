from setuptools import setup, find_packages

setup(
    name='postgres-rabbitmq',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'producer = producer:producer',
            'event_logger = consumer:log_event',
        ],
    },
)

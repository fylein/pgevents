from setuptools import setup, find_packages

setup(
    name='fyle-pgevents',
    version='0.1',
    author='Aditya Agrawal',
    author_email='aditya999123@gmail.com',
    description='small description here',
    long_description='long description here',
    long_description_content_type='text/markdown',
    url='https://github.com/fylein/fyle-pgevents',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    package_dir={
        '': 'src'
    },
    packages=find_packages(where='src'),
    python_requires=">=3.8",
    install_requires=[
        'pika==1.1.0',
        'psycopg2==2.8.6',
        'click==7.0',
        'brotli==1.0.9'
    ]
)

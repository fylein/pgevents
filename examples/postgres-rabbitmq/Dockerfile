FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y libpq-dev gcc

#================================================================
# pip install required modules
#================================================================

RUN pip install --upgrade setuptools
ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

#==================================================
# Add the latest code
#==================================================

RUN mkdir -p /postgres-rabbitmq
WORKDIR /postgres-rabbitmq

ADD . /postgres-rabbitmq
RUN pip install ./fyle-pgevents
ADD producer.py /postgres-rabbitmq

RUN pip install -e .
CMD ["/bin/bash"]

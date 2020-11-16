FROM python:3.9-slim

# RUN apt-get update && \
#     apt-get install -y openssh-client gnupg git

#################################################
# Install requirements
#################################################

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

#################################################
# Copy over code
#################################################

RUN mkdir -p /fyle-pg-to-rabbitmq

WORKDIR /fyle-pg-to-rabbitmq

COPY . /fyle-pg-to-rabbitmq/

ENTRYPOINT [ "python", "pg-to-rabbitmq.py" ]
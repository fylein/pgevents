FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y libpq-dev gcc

#################################################
# Install requirements
#################################################

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

#################################################
# Copy over code
#################################################

RUN mkdir -p /fyle-pg-recvlogical

WORKDIR /fyle-pg-recvlogical

COPY . /fyle-pg-recvlogical/
#RUN pip install .

ENV PYTHONUNBUFFERED 1
CMD ["/bin/bash"]

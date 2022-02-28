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

RUN mkdir -p /fyle-pgevents
WORKDIR /fyle-pgevents

ADD . /fyle-pgevents

RUN pip install -e .
CMD ["/bin/bash"]

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

RUN mkdir -p /pgevents
WORKDIR /pgevents

ADD . /pgevents

# Make health check script executable
RUN chmod +x /pgevents/producer/health_check.sh

RUN pip install -e .
CMD ["/bin/bash"]

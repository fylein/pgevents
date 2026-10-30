FROM python:3.14-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq-dev gcc libc6-dev postgresql-client-17 && \
    rm -rf /var/lib/apt/lists/*

#================================================================
# pip install required modules
#================================================================

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip setuptools && \
    pip install --no-cache-dir -r /tmp/requirements.txt

#==================================================
# Add the latest code
#==================================================

WORKDIR /pgevents

COPY . .

# Make health check script executable
RUN chmod +x /pgevents/producer/health_check.sh

RUN pip install --no-cache-dir -e .
CMD ["/bin/bash"]

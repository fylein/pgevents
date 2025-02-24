FROM postgres:12

RUN export PATH=/usr/lib/postgresql/12/bin:$PATH

# Copy the database initialization script:
COPY ./fixtures/* /docker-entrypoint-initdb.d/
COPY ./init-pgconf.sh /docker-entrypoint-initdb.d/init-pgconf.sh

# Install wal2json
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-12-wal2json \
    && rm -rf /var/lib/apt/lists/*
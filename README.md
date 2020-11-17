# fyle-pg-recvlogical

Utility to generate events from PG using logical replication and push it to Rabbitmq.

# Build

Easiest way is to use docker.

```
docker build -t fyle-pg-recvlogical .
```

# Usage

Set the following environment variables to connect to PostgreSQL >= 10 and to RabbitMQ as broker

```
export PGHOST=xxx
export PGPORT=5432
export PGDATABASE=test
export PGUSER=postgres
export PGPASSWORD=xxx
export PGSLOT=test_slot

export RABBITMQ_URL=yyy
export RABBITMQ_EXCHANGE=table_exchange
export RABBITMQ_QUEUE_NAME=rabbitmq_to_stdout

```

Note that if you're running on Docker on Mac and want to connect to host machine, then set PGHOST to host.docker.internal and not localhost or 127.0.0.1.

To read from PostgreSQL and send output to stdout (use this for debugging)

```
docker run -i -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSLOT --rm fyle-pg-recvlogical pg_to_stdout
```

To read from PostgreSQL and send data to rabbitmq
```
docker run -i -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSLOT -e RABBITMQ_URL -e RABBITMQ_EXCHANGE --rm fyle-pg-recvlogical pg_to_rabbitmq
```

To read data from rabbitmq exchange and print it to stdout
```
docker run -i -e RABBITMQ_URL -e RABBITMQ_EXCHANGE -e RABBITMQ_QUEUE_NAME --rm fyle-pg-recvlogical rabbitmq_to_stdout
```

For detailed help, run the following command

```
$ docker run -i -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSLOT --rm fyle-pg-recvlogical pg_to_rabbitmq --help
Usage: pg_to_rabbitmq [OPTIONS]

Options:
  --pghost TEXT             Postgresql Host ($PGHOST)  [required]
  --pgport TEXT             Postgresql Host ($PGPORT)  [required]
  --pgdatabase TEXT         Postgresql Database ($PGDATABASE)  [required]
  --pguser TEXT             Postgresql User ($PGUSER)  [required]
  --pgpassword TEXT         Postgresql Password ($PGPASSWORD)  [required]
  --pgslot TEXT             Postgresql Replication Slot Name ($PGSLOT)
                            [required]
  --whitelist-regex TEXT    Regex of schema.table to include - e.g. .*\.foo
  --blacklist-regex TEXT    Regex of schema.table to exclude - e.g. testns\..*
  --rabbitmq-url TEXT       RabbitMQ url ($RABBITMQ_URL)  [required]
  --rabbitmq-exchange TEXT  RabbitMQ exchange ($RABBITMQ_EXCHANGE)  [required]
  --help                    Show this message and exit.
```

# Development

Map the volume to the docker container and run the utility from within the container while you're making changes in the editor:

```
docker run -it -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSLOT -e RABBITMQ_URL -e RABBITMQ_EXCHANGE -e RABBITMQ_QUEUE_NAME --rm -v $(pwd):/fyle-pg-recvlogical --entrypoint=/bin/bash fyle-pg-recvlogical
```

Now make changes to the python files. Then run the command from shell:

```
python pg_to_rabbitmq.py
```


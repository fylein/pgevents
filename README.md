# fyle-pg-recvlogical

Utility to generate events from PG using logical replication and push it to Rabbitmq.

# Build

Easiest way is to use docker.

```
docker build -t fyle-pg-recvlogical .
```

# Usage

Set the following environment variables to connect to PostgreSQL >= 10

```
export PGHOST=xxx
export PGPORT=5432
export PGDATABASE=test
export PGUSER=postgres
export PGPASSWORD=xxx
export PGSLOT=test_slot
```

Note that if you're running on Docker on Mac and want to connect to host machine, then set PGHOST to host.docker.internal and not localhost or 127.0.0.1.

Then run the utility to print output to stdout
```
docker run -i -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSLOT --rm fyle-pg-recvlogical stdout_writer
```

To send data to rabbitmq
```
export RABBITMQ_URL=yyy
export RABBITMQ_EXCHANGE=copy
docker run -i -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSLOT -e RABBITMQ_URL -e RABBITMQ_EXCHANGE --rm fyle-pg-recvlogical rabbitmq_writer
```

To read data from rabbitmq exchange
```
export RABBITMQ_URL=yyy
export RABBITMQ_EXCHANGE=copy
docker run -i -e RABBITMQ_URL -e RABBITMQ_EXCHANGE --rm fyle-pg-recvlogical rabbitmq_reader
```

# Development

Map the volume to the docker container and run the utility from within the container while you're making changes in the editor:

```
docker run -it -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSLOT -e RABBITMQ_URL -e RABBITMQ_EXCHANGE --rm -v $(pwd):/fyle-pg-recvlogical --entrypoint=/bin/bash fyle-pg-recvlogical
```

# Help

## Stdout

```
# python stdout.py --help
Usage: stdout.py [OPTIONS]

Options:
  --pghost TEXT           Postgresql Host ($PGHOST)  [required]
  --pgport TEXT           Postgresql Host ($PGPORT)  [required]
  --pgdatabase TEXT       Postgresql Database ($PGDATABASE)  [required]
  --pguser TEXT           Postgresql User ($PGUSER)  [required]
  --pgpassword TEXT       Postgresql Password ($PGPASSWORD)  [required]
  --pgslot TEXT           Postgresql Replication Slot Name ($PGSLOT)
                          [required]
  --whitelist-regex TEXT  Regex of schema.table to include - e.g. .*\.foo
  --blacklist-regex TEXT  Regex of schema.table to exclude - e.g. testns\..*
  --help                  Show this message and exit.

```

## Rabbitmq

```
# python rabbitmq.py --help
Usage: rabbitmq.py [OPTIONS]

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
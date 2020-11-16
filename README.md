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
```

Then run the utility to print output to stdout
```
docker run -i -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD --rm fyle-pg-recvlogical stdout
```

To send data to rabbitmq
```
export RABBITMQ_URL=yyy
docker run -i -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e RABBITMQ_URL --rm fyle-pg-recvlogical rabbitmq
```

# Development

Map the volume to the docker container and run the utility from within the container while you're making changes in the editor:

```
docker run -it -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e RABBITMQ_URL --rm -v $(pwd):/fyle-pg-recvlogical --entrypoint=/bin/bash fyle-pg-recvlogical
```

Then within the shell

```
python stdout.py --pghost=foo
```

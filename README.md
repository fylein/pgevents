# fyle-pg-recvlogical

Utility to generate events from PG using logical replication and push it to Rabbitmq.

# Build

Easiest way is to use docker.

```
docker build -t fyle-pg-recvlogical .
```

# Usage

Set the following environment variables to connect to PostgreSQL >= 10.

```
export PGHOST=xxx
export PGPORT=5432
export PGDATABASE=test
export PGUSER=postgres
export PGPASSWORD=xxx
```

Set the following environment variables to connect to Rabbitmq

```
export RABBITMQ_URL=yyy
```

Then run the utility
```
docker run -i -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD --rm fyle-pg-recvlogical stdout
```

# Development

Map the volume to the docker container and run the utility from within the container while you're making changes in the editor:

```
docker run -it -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD --rm -v $(pwd):/fyle-pg_recvlogical --entrypoint=/bin/bash fyle-pg-recvlogical
```

Then within the shell

```
python -m stdout
```

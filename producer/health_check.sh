#!/bin/bash

# Check PgBouncer connection using pg_isready
pg_isready -h pgbouncer -p 6432 -U $PGUSER -d $PGDATABASE
PGBOUNCER_STATUS=$?

if [ $PGBOUNCER_STATUS -ne 0 ]; then
    echo "PgBouncer connection failed"
    exit 1
fi

# Check direct PostgreSQL connection using pg_isready
pg_isready -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE
POSTGRES_STATUS=$?

if [ $POSTGRES_STATUS -ne 0 ]; then
    echo "PostgreSQL connection failed"
    exit 1
fi

echo "Both PgBouncer and PostgreSQL connections are healthy"
exit 0

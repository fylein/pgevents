FROM postgres:12-alpine

RUN apk add --no-cache gcc make clang libc-dev llvm

COPY ./wal2json-wal2json_2_3.tar.gz wal2json-wal2json_2_3.tar.gz

RUN tar -zxf wal2json-wal2json_2_3.tar.gz

COPY ./fixtures/* /docker-entrypoint-initdb.d/

RUN export PATH=/usr/lib/postgresql/12/bin:$PATH

RUN cd wal2json-wal2json_2_3 \
    && make \
    && make install

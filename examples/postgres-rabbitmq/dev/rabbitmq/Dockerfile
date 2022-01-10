FROM rabbitmq:3.8.9-management

ENV RABBITMQ_MNESIA_BASE /data/mnesia
ENV RABBITMQ_NODENAME rabbitmq@localhost

RUN rabbitmq-plugins enable rabbitmq_prometheus

ADD rabbitmq.conf /etc/rabbitmq
ADD . /rabbitmq
WORKDIR /rabbitmq

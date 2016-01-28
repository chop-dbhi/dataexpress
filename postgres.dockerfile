#
# Postgres Dockerfile
#

# Pull base image

FROM postgres:9.4

RUN mkdir -p /opt/apps/ops

COPY ./ops/* /opt/apps/ops/

ADD ./ops/test/postgres/setup_postgres.sh /usr/local/bin/setup_postgres

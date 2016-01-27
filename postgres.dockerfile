#
# Postgres Dockerfile
#

# Pull base image

FROM postgres:9.4

RUN echo "" >> $PGDATA/postgresql.conf && \
    echo "ssl on" >> $PGDATA/postgreesql.conf

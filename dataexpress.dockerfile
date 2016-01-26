#
# Scala and sbt Dockerfile
#
# https://github.com/hseeberger/scala-sbt
#

# Pull base image
FROM java:8

ENV SCALA_VERSION 2.11.7
ENV SBT_VERSION 0.13.8

# Install Scala
## Piping curl directly in tar
RUN \
  apt-get update && \
  curl -fsL http://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /root/ && \
  echo >> /root/.bashrc && \
  echo 'export PATH=~/scala-$SCALA_VERSION/bin:$PATH' >> /root/.bashrc

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get install sbt

RUN apt-get install -y  sqlite3 \
                        vim \
                        memcached

RUN mkdir -p /opt/apps/dataexpress/build
ADD . /opt/apps/dataexpress/
ADD . /opt/apps/dataexpress/build/
ADD src /opt/apps/dataexpress/build/src

ENV APP_DIR /opt/apps/dataexpress/
ENV PATH /opt/apps/dataexpress/bin:$PATH

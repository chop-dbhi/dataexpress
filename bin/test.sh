#!/bin/bash

export PATH=$HOME/scala-$SCALA_VERSION/bin::$PATH

cd $APP_DIR/build

make_test_properties

sbt ++2.11.7 'test-only * -- -l "edu.chop.cbmi.dataExpress.test.util.tags.PostgresqlTest"'

#!/bin/bash

make_test_properties

cd $APP_DIR/build

sbt ++2.11.7 'test-only * -- -l "edu.chop.cbmi.dataExpress.test.util.tags.PostgresqlTest"'

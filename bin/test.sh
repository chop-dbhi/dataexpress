#!/bin/bash

./make_props.sh

cd $APP_DIR/build

#sbt ++2.11.7 'test-only * -- -l "edu.chop.cbmi.dataExpress.test.util.tags.PostgresqlTest edu.chop.cbmi.dataExpress.test.util.tags.OracleTest"'

sbt ++2.11.7 'test-only * -- -l "edu.chop.cbmi.dataExpress.test.util.tags.PostgresqlTest edu.chop.cbmi.dataExpress.test.util.tags.OracleTest"'

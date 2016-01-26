#!/bin/bash

sbt ++2.11.7 'test-only * -- -l "edu.chop.cbmi.dataExpress.test.util.tags.SqlServerTest edu.chop.cbmi.dataExpress.test.util.tags.OracleTest"'
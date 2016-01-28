#!/bin/bash

cp $TEST_CONFIG_DIR/*.* $PGDATA/

chmod 640 $PGDATA/server.crt

chmod 640 $PGDATA/server.key

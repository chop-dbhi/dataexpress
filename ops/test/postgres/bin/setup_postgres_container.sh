#!/bin/bash

cp $HOST_TEST_CONFIG_DIR/*.* $PGDATA/

chmod 640 $PGDATA/server.crt

chmod 640 $PGDATA/server.key

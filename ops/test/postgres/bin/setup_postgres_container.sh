#!/bin/bash

export HOST_TEST_CONFIG_DIR=./ops/test/postgres/config

export HOST_TEST_DATA_DIR=./ops/test/postgres/data

cp $HOST_TEST_CONFIG_DIR/*.* $HOST_TEST_DATA_DIR/

chmod 640 $HOST_TEST_DATA_DIR/server.crt

chmod 640 $HOST_TEST_DATA_DIR/server.key

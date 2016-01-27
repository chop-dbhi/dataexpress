#!/bin/bash

cp $APP_DIR/test/config/postgres/*.* $APP_DIR/test/config/postgres/data/

chmod 640 $APP_DIR/test/config/postgres/data/server.*

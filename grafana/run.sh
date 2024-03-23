#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
echo "Config located in $SCRIPT_DIR"

docker run -v $SCRIPT_DIR/grafana.ini:/etc/grafana/grafana.ini -p 3000:3000 -e GF_LOG_LEVEL=debug -e GF_DATABASE_LOG_QUERIES=true grafana/grafana-oss:latest

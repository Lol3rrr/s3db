#!/bin/bash

set -xe;

pgbench -h localhost -i;
pgbench -h localhost -T 30 --progress 1;

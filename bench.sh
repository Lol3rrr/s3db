#!/bin/bash

pgbench -h localhost -i;
pgbench -h localhost -T 30;

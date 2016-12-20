#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#
set -e

docker run -it --rm  \
       -v $PWD:/usr/src/mymaven \
       -v $PWD/.m2:/root/.m2 \
       -w /usr/src/mymaven \
       maven:alpine mvn clean "$@"

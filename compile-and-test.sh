#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#

docker rm influxdb
docker run -d --name influxdb -p 8086:8086 influxdb

docker run -it --rm  \
       -v $PWD:/usr/src/mymaven \
       -v $PWD/.m2:/root/.m2 \
       -w /usr/src/mymaven \
       --link=influxdb \
       -e INFLUXDB_IP=influxdb \
       maven mvn clean install

docker kill influxdb

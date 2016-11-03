#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#
set -e

INFLUXDB_VERSIONS="0.13-alpine 1.0-alpine 1.1.0-rc1-alpine"

for version in ${INFLUXDB_VERSIONS}
do
  echo "Tesing againts influxdb ${version}"
  docker rm influxdb
  docker run -d --name influxdb -p 8086:8086 influxdb:${version}

  docker run -it --rm  \
         -v $PWD:/usr/src/mymaven \
         -v $PWD/.m2:/root/.m2 \
         -w /usr/src/mymaven \
         --link=influxdb \
         -e INFLUXDB_IP=influxdb \
         maven mvn clean install

  docker kill influxdb
done

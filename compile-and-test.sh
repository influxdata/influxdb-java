#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#
set -e

INFLUXDB_VERSIONS="0.13-alpine 1.0-alpine 1.1-alpine"

for version in ${INFLUXDB_VERSIONS}
do
  echo "Tesing againts influxdb ${version}"
  docker kill influxdb || true
  docker rm influxdb || true
  docker run \
            --detach \
            --name influxdb \
            --publish 8086:8086 \
            --publish 8089:8089/udp \
            --volume ${PWD}/influxdb.conf:/etc/influxdb/influxdb.conf \
        influxdb:${version}

  docker run -it --rm  \
        --volume $PWD:/usr/src/mymaven \
        --volume $PWD/.m2:/root/.m2 \
        --workdir /usr/src/mymaven \
        --link=influxdb \
        --env INFLUXDB_IP=influxdb \
         maven mvn clean install

  docker kill influxdb || true
done

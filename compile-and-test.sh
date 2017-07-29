#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#
set -e

INFLUXDB_VERSIONS="1.3 1.2 1.1"

JAVA_VERSIONS="3-jdk-8 3-jdk-9"


for java_version in ${JAVA_VERSIONS}
do
 echo "Run tests with maven:${java_version}"
for version in ${INFLUXDB_VERSIONS}
do
  echo "Tesing againts influxdb ${version}"
  docker kill influxdb || true
  docker rm influxdb || true
  docker pull influxdb:${version}-alpine || true
  docker run \
            --detach \
            --name influxdb \
            --publish 8086:8086 \
            --publish 8089:8089/udp \
            --volume ${PWD}/influxdb.conf:/etc/influxdb/influxdb.conf \
        influxdb:${version}-alpine

  docker run -it --rm  \
        --volume $PWD:/usr/src/mymaven \
        --volume $PWD/.m2:/root/.m2 \
        --workdir /usr/src/mymaven \
        --link=influxdb \
        --env INFLUXDB_IP=influxdb \
         maven:${java_version} mvn clean install

  docker kill influxdb || true
done
done

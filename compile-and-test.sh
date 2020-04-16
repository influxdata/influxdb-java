#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#
set -e

DEFAULT_INFLUXDB_VERSION="1.8"
DEFAULT_MAVEN_JAVA_VERSION="3-jdk-14"

INFLUXDB_VERSION="${INFLUXDB_VERSION:-$DEFAULT_INFLUXDB_VERSION}"
MAVEN_JAVA_VERSION="${MAVEN_JAVA_VERSION:-$DEFAULT_MAVEN_JAVA_VERSION}"

echo "Run tests with maven:${MAVEN_JAVA_VERSION} on influxdb-${INFLUXDB_VERSION}"
docker kill influxdb || true
docker rm influxdb || true
docker pull influxdb:${INFLUXDB_VERSION}-alpine || true
docker run \
       --detach \
       --name influxdb \
       --publish 8086:8086 \
       --publish 8089:8089/udp \
       --volume ${PWD}/influxdb.conf:/etc/influxdb/influxdb.conf \
       influxdb:${INFLUXDB_VERSION}-alpine

echo "Starting Nginx"
docker kill nginx || true
docker rm  nginx || true

docker run \
       --detach \
       --name nginx \
       --publish 8080:8080 \
       --publish 8080:8080/udp \
       --volume ${PWD}/src/test/nginx/nginx.conf:/etc/nginx/nginx.conf:ro \
       --link influxdb:influxdb \
       nginx:stable-alpine nginx '-g' 'daemon off;'

echo "Running tests"
PROXY_API_URL=http://nginx:8080/influx-api/
PROXY_UDP_PORT=8080

docker run -it --rm \
       --volume ${PWD}:/usr/src/mymaven \
       --volume ${PWD}/.m2:/root/.m2 \
       --workdir /usr/src/mymaven \
       --link=influxdb \
       --link=nginx \
       --env INFLUXDB_VERSION=${INFLUXDB_VERSION} \
       --env INFLUXDB_IP=influxdb \
       --env PROXY_API_URL=${PROXY_API_URL} \
       --env PROXY_UDP_PORT=${PROXY_UDP_PORT} \
       maven:${MAVEN_JAVA_VERSION} mvn clean install

docker kill influxdb || true
docker kill nginx || true

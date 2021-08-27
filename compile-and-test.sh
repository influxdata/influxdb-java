#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#
set -e

DEFAULT_INFLUXDB_VERSION="1.8"
DEFAULT_MAVEN_JAVA_VERSION="3-openjdk-16-slim"

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
       --env DOCKER_INFLUXDB_INIT_MODE=setup \
       --env DOCKER_INFLUXDB_INIT_USERNAME=my-user \
       --env DOCKER_INFLUXDB_INIT_PASSWORD=my-password \
       --env DOCKER_INFLUXDB_INIT_ORG=my-org \
       --env DOCKER_INFLUXDB_INIT_BUCKET=my-bucket \
       --env DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=my-token \
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
if [[ "$INFLUXDB_VERSION" == "2.0" ]]
then
    TEST_EXPRESSION="InfluxDB2Test"
    # Wait to start InfluxDB
    docker run --link influxdb:influxdb ubuntu:20.04 bash -c "apt-get update \
      && apt-get install wget --yes \
      && wget -S --spider --tries=20 --retry-connrefused --waitretry=5 http://influxdb:8086/ping"
    # Create DBRP Mapping
    BUCKET_ID=$(docker exec influxdb bash -c "influx bucket list -o my-org -n my-bucket | grep my-bucket | xargs | cut -d ' ' -f 0")
    docker exec influxdb bash -c "influx v1 dbrp create -o my-org --db mydb --rp autogen --default --bucket-id ${BUCKET_ID}"
    docker exec influxdb bash -c "influx v1 auth create -o my-org --username my-user --password my-password --read-bucket ${BUCKET_ID} --write-bucket ${BUCKET_ID}"
else
    TEST_EXPRESSION="*"
fi

docker run --rm \
       --volume ${PWD}:/usr/src/mymaven \
       --volume ${PWD}/.m2:/root/.m2 \
       --workdir /usr/src/mymaven \
       --link=influxdb \
       --link=nginx \
       --env INFLUXDB_VERSION=${INFLUXDB_VERSION} \
       --env INFLUXDB_IP=influxdb \
       --env PROXY_API_URL=${PROXY_API_URL} \
       --env PROXY_UDP_PORT=${PROXY_UDP_PORT} \
       maven:${MAVEN_JAVA_VERSION} mvn clean install -Dtest="${TEST_EXPRESSION}"

docker kill influxdb || true
docker kill nginx || true

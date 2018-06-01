#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#
# Note for Windows users:
#   In case your docker still uses VirtualBox as a VM, you will probably have to
#   inform the containers about the location of your repository.
#   Please not that Docker for windows, enables you to mount everything
#   from your Users (C:\Users in most cases) directory, so to keep it easy,
#   it's better to keep your repository somewhere there.
#   If you will decide to put your sources somewhere else, please visit your
#   VirtualBox settings and check out the "Shared folder configuration".
#   This script uses environment variable BUILD_HOME which should point to this 
#   project directory (i.e. //c/Users/MyWindowsUserName/Projects/influxdb-java)
#
#   Of course you still need bash to launch this script. But this should be no 
#   problem either to install it (this script was tested with GitExtensions package).

set -e

DEFAULT_INFLUXDB_VERSION="1.5"
DEFAULT_MAVEN_JAVA_VERSION="3-jdk-10-slim"

INFLUXDB_VERSION="${INFLUXDB_VERSION:-$DEFAULT_INFLUXDB_VERSION}"
MAVEN_JAVA_VERSION="${MAVEN_JAVA_VERSION:-$DEFAULT_MAVEN_JAVA_VERSION}"

WORKDIR=/usr/src/mymaven

if [ -z "$BUILD_HOME" ] ; then
    BUILD_HOME=$PWD
    if [ -x  /c/Windows/System32/ ] ; then
    BUILD_HOME=/$PWD
    fi
fi

if [ -x  /c/Windows/System32/ ] ; then
    echo "Detected Windows as a host system"
    WORKDIR=//usr/src/mymaven
fi

echo Using build home: $BUILD_HOME

function run_test {
    USE_PROXY=$1
    
    INFLUXDB_API_URL=http://influxdb:8086
    if [ "$USE_PROXY" == "nginx" ] ; then
    echo Test with Nginx as proxy
    INFLUXDB_API_URL=http://nginx:8080/influx-api/
    fi

    
    echo "Run tests with maven:${MAVEN_JAVA_VERSION} on onfluxdb-${INFLUXDB_VERSION}"
    docker kill influxdb || true
    docker rm influxdb || true
    docker pull influxdb:${version}-alpine || true
    docker run \
       --detach \
       --name influxdb \
       --publish 8086:8086 \
       --publish 8089:8089/udp \
       --volume ${BUILD_HOME}/influxdb.conf:/etc/influxdb/influxdb.conf \
       influxdb:${INFLUXDB_VERSION}-alpine

    if [ "$USE_PROXY" == "nginx" ] ; then
    echo Starting Nginx
    docker kill nginx || true        
    docker rm  nginx || true
    echo ----- STARTING NGINX CONTAINER -----
    docker run \
           --detach \
           --name nginx \
           --publish 8888:8080 \
           --volume ${BUILD_HOME}/src/test/nginx/nginx.conf:/etc/nginx/conf.d/default.conf:ro \
           --link influxdb:influxdb \
           nginx nginx '-g' 'daemon off;'
    
    NGINX_LINK=--link=nginx
    SKIP_TESTS=-DsomeModule.test.excludes="**/*UDPInfluxDBTest*"
    fi
    
    docker run -it --rm  \
       --volume $BUILD_HOME:/usr/src/mymaven \
       --volume $BUILD_HOME/.m2:/root/.m2 \
       --workdir $WORKDIR \
       --link=influxdb $NGINX_LINK \
       --env INFLUXDB_API_URL=$INFLUXDB_API_URL \
       maven:${MAVEN_JAVA_VERSION} mvn clean install $SKIP_TESTS

    docker kill influxdb || true
    docker kill nginx || true
    docker rm -f nginx || true
}

run_test
run_test nginx

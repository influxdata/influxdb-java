#!/bin/bash

docker run \
            --detach \
            --name influxdb \
            --publish 8086:8086 \
            --publish 8089:8089/udp \
            --volume ${PWD}/influxdb.conf:/etc/influxdb/influxdb.conf \
	influxdb:latest

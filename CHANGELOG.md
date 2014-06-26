## v1.2 [unreleased]

#### Tasks

- [Issue #2] (https://github.com/influxdb/influxdb-java/issues/2) Implement the last missing api calls ( interfaces, sync, forceCompaction, servers, shards)
- [Issue #8] (https://github.com/influxdb/influxdb-java/issues/8) Use com.github.docker-java which replaces com.kpelykh for Integration tests.
- [Issue #6] (https://github.com/influxdb/influxdb-java/issues/6) Update Retrofit to 1.6.0 
- [Issue #7] (https://github.com/influxdb/influxdb-java/issues/7) Update Guava to 17.0 
- use (http://square.github.io/okhttp/, okhttp) instead of java builtin httpconnection to get failover for the http endpoint.
- fix dependency to guava.

## v1.1 [2014-05-31]

#### Features

- Add InfluxDB#version() to get the InfluxDB Server version information.
- Changed InfluxDB#createDatabase() to match (https://github.com/influxdb/influxdb/issues/489) without replicationFactor.
- Updated Retrofit from 1.5.0 -> 1.5.1

## v1.0 [2014-05-6]

  * Initial Release

## v2.3 [unreleased]

#### Features


## v2.2 [2016-04-11]

#### Features

 - Allow writing of pre-constructed line protocol strings

#### Fixes

 - Correct escaping of database names for create and delete database actions
 - Many bug fixes / improvements in general

#### Other
 - Deprecated `field()` method in preference for `addField()` methods.

## v2.1 [2015-12-05]

#### Features

 - Extensions to fluent builder classes
 - Convenience methods for building Points
 - Allow integer types as field values

#### Fixes

 - Fixed escaping of tag and field values
 - Always uses nanosecond precision for time
 - Uses NumberFormat class for safer formatting of large numbers.

## v2.0 [2015-07-17]

#### Features

- Compatible with InfluxDB version 0.9+
- Support for lineprotocol
- Batched write support

## v1.4 [2014-11-11] / v1.5 [2014-11-14]

No major functional changes or improvements. Mainly library updates and code structure

## v1.3 [2014-10-22]

#### Features

- Compatible with InfluxDB Version up to 0.8
- API: add a InfluxDB#createDatabase(DatabaseConfiguration) to be able to create a new Database with ShardSpaces defined.
- API: introduction of InfluxDB#createShardSpare, InfluxDB#getShardSpace and InfluxDB#dropShardSpace
- API: deprecated InfluxDB#createShard, InfluxDB#getShards and InfluxDB#dropShard, this is replaced with shardSpaces in InfluxDB >= 0.8.0
- API: renamed InfluxDB#deletePoints to InfluxDB#deleteSeries because this is what it actually does.
- [Issue #14] update docker-java for tests to 0.10.0 
- Update retrofit from 1.6.0 -> 1.6.1
- Use ms instead of m for millisecond timeprecision.

## v1.2 [2014-06-28]

#### Features

- [Issue #2] (https://github.com/influxdb/influxdb-java/issues/2) Implement the last missing api calls ( interfaces, sync, forceCompaction, servers, shards)
- use (http://square.github.io/okhttp/, okhttp) instead of java builtin httpconnection to get failover for the http endpoint.

#### Tasks

- [Issue #8] (https://github.com/influxdb/influxdb-java/issues/8) Use com.github.docker-java which replaces com.kpelykh for Integration tests.
- [Issue #6] (https://github.com/influxdb/influxdb-java/issues/6) Update Retrofit to 1.6.0 
- [Issue #7] (https://github.com/influxdb/influxdb-java/issues/7) Update Guava to 17.0 
- fix dependency to guava.

## v1.1 [2014-05-31]

#### Features

- Add InfluxDB#version() to get the InfluxDB Server version information.
- Changed InfluxDB#createDatabase() to match (https://github.com/influxdb/influxdb/issues/489) without replicationFactor.
- Updated Retrofit from 1.5.0 -> 1.5.1

## v1.0 [2014-05-6]

  * Initial Release

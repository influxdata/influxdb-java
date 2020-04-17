# Changelog

## 2.19 [unreleased]

## 2.18 [2020-04-17]

### Fixes

- Update to okhttp 4.x [PR #644](https://github.com/influxdata/influxdb-java/pull/644)

## 2.17 [2019-12-06]

### Fixes

- Fixed runtime exception propagation in chunked query [Issue #639](https://github.com/influxdata/influxdb-java/issues/639)

## 2.16 [2019-10-25]

### Fixes

- Add new annotation called TimeColumn for timestamp field in POJO bean, this can set Point time and precision field correctly, also avoid UnableToParseException when flush Point to influx.
- Skip fields with NaN and infinity values when writing to InfluxDB
  [Issue #614](https://github.com/influxdata/influxdb-java/issues/614)

## 2.15 [2019-02-22]

### Fixes

- Close underlying OkHttpClient when closing [Issue #359](https://github.com/influxdata/influxdb-java/issues/359)
- Update OkHttp to 3.13.1 which disables TLSv1 and TLSv1.1 by default, if still required you can enable them:

```java
OkHttpClient client = new OkHttpClient.Builder()
    .connectionSpecs(Arrays.asList(ConnectionSpec.COMPATIBLE_TLS))
    .build();
```

### Features

- Query and BatchPoints do not mandate a database name, in which case the InfluxDB database
  would be used [Issue #548](https://github.com/influxdata/influxdb-java/issues/548)
- Add BatchPoints.Builder.points(Collection)
  [Issue #451](https://github.com/influxdata/influxdb-java/issues/451)
- @Column supports class inheritance
  [Issue #367](https://github.com/influxdata/influxdb-java/issues/367)
- BatchOptions to have .precision()
  [Issue #532](https://github.com/influxdata/influxdb-java/issues/532)
- Point.Builder.addFieldsFromPOJO to add Column fields from super class
  [Issue #613](https://github.com/influxdata/influxdb-java/issues/613)

## 2.14 [2018-10-12]

### Fixes

- Fixed chunked query exception handling [Issue #523](https://github.com/influxdata/influxdb-java/issues/523)
- Memory leak in StringBuilder cache for Point.lineprotocol() [Issue #526](https://github.com/influxdata/influxdb-java/issues/521)

## 2.13 [2018-09-12]

### Fixes
- MessagePack queries: Exception during parsing InfluxDB version [macOS] [PR #487](https://github.com/influxdata/influxdb-java/issues/487)
- The InfluxDBResultMapper is able to handle results with a different time precision [PR #501](https://github.com/influxdata/influxdb-java/pull/501)
- UDP target host address is cached [PR #502](https://github.com/influxdata/influxdb-java/issues/502)
- Error messages from server not parsed correctly when using msgpack [PR #506](https://github.com/influxdata/influxdb-java/issues/506)
- Response body must be closed properly in case of JSON response [PR #514](https://github.com/influxdata/influxdb-java/issues/514)
- Time is serialized not consistently in MsgPack and Json, missing millis and nanos in MsgPack[PR #517](https://github.com/influxdata/influxdb-java/issues/517)

### Features

- Support for Basic Authentication [PR #492](https://github.com/influxdata/influxdb-java/pull/492)
- Added possibility to reuse client as a core part of [influxdb-java-reactive](https://github.com/bonitoo-io/influxdb-java-reactive) client [PR #493](https://github.com/influxdata/influxdb-java/pull/493)
- Retry capability for writing of BatchPoints [PR #503](https://github.com/influxdata/influxdb-java/issues/503)
- Added `BiConsumer` with capability to discontinue a streaming query [Issue #515](https://github.com/influxdata/influxdb-java/issues/515)
- Added `onComplete` action that is invoked after successfully end of streaming query [Issue #515](https://github.com/influxdata/influxdb-java/issues/515)

## 2.12 [2018-07-31]

### Fixes

- Remove code which checks for unsupported influxdb versions [PR #474](https://github.com/influxdata/influxdb-java/pull/474)
- Unpredictable errors when OkHttpClient.Builder instance is reused [PR #478](https://github.com/influxdata/influxdb-java/pull/478)

### Features

- Support for MessagePack [PR #471](https://github.com/influxdata/influxdb-java/pull/471)
- Cache version per influxdb instance and reduce ping() calls for every query call [PR #472](https://github.com/influxdata/influxdb-java/pull/472)
- FAQ list for influxdb-java [PR #475](https://github.com/influxdata/influxdb-java/pull/475)

### Improvements

- Test: Unit test to ensure tags should be sorted by key in line protocol (to reduce db server overheads) [PR #476](https://github.com/influxdata/influxdb-java/pull/476)

## 2.11 [2018-07-02]

### Features

- Allow write precision of TimeUnit other than Nanoseconds [PR #321](https://github.com/influxdata/influxdb-java/pull/321)
- Support dynamic measurement name in InfluxDBResultMapper [PR #423](https://github.com/influxdata/influxdb-java/pull/423)
- Debug mode which allows HTTP requests being sent to the database to be logged [PR #450](https://github.com/influxdata/influxdb-java/pull/450)
- Fix problem of connecting to the influx api with URL which does not points to the url root (e.g. localhots:80/influx-api/) [PR #400] (https://github.com/influxdata/influxdb-java/pull/400)

## 2.10 [2018-04-26]

### Fixes
- Fix IllegalAccessException on setting value to POJOs, InfluxDBResultMapper is now more thread-safe [PR #432](https://github.com/influxdata/influxdb-java/pull/432)

### Features

- Support for parameter binding in queries ("prepared statements") [PR #429](https://github.com/influxdata/influxdb-java/pull/429)
- Allow to figure out whether the Point.Builder has any field or not [PR #434](https://github.com/influxdata/influxdb-java/pull/434)

### Improvements

- Performance: use chained StringBuilder calls instead of single calls [PR #426](https://github.com/influxdata/influxdb-java/pull/426)
- Performance: Escape fields and keys more efficiently [PR #424](https://github.com/influxdata/influxdb-java/pull/424)
- Build: Speed up travis build [PR #435](https://github.com/influxdata/influxdb-java/pull/435)
- Test: Update junit from 5.1.0 to 5.1.1 [PR #441](https://github.com/influxdata/influxdb-java/pull/441)

## 2.9 [2018-02-27]

### Features

- New extensible API to configure batching properties.  [PR #409] 
- New configuration property 'jitter interval' to avoid multiple clients hit the server periodically at the same time. [PR #409] 
- New strategy on handling errors, client performs retries writes when server gets overloaded [PR #410]
- New exceptions give the client user easier way to classify errors reported by the server. [PR #410] 

## 2.8 [2017-12-06]

### Fixes

- InfluxDBResultMapper now is able to process QueryResult created when a GROUP BY clause was used [PR #345](https://github.com/influxdata/influxdb-java/pull/345)
- InfluxDB will now handle the timestamp on its own if none is provided [PR#350](https://github.com/influxdata/influxdb-java/pull/350)

### Features

- API: add InfluxDB#createRetentionPolicy and InfluxDB#dropRetentionPolicy to be able to create and drop Retention Policies [PR #351](https://github.com/influxdata/influxdb-java/pull/351)
- API: add InfluxDB#query that uses callbacks

### Improvements

- Build: all unit and integration test are now running with jdk8 and jdk9.
- Test: migration to junit5

## v2.7 [2017-06-26]

### Features

- Simplify write() methods for use cases writing all points to same database and retention policy [PR #327](https://github.com/influxdata/influxdb-java/pull/327)
- QueryResult to Object mapper added [PR #341](https://github.com/influxdata/influxdb-java/pull/341)

### Fixes

- Replace RuntimeException with InfluxDBException [Issue #323](https://github.com/influxdata/influxdb-java/issues/323)

### Improvements

- Significant (~35%) performance improvements for write speed with less memory footprint. [PR #330](https://github.com/influxdata/influxdb-java/pull/330)
- Drop guava runtime dependency which reduces jar size from 1MB -> 49KB [PR #322](https://github.com/influxdata/influxdb-java/pull/322)

## v2.6 [2017-06-08]

### Features

- Switch to Java 1.8
- Support chunking
- Add a databaseExists method to InfluxDB interface
- [Issue #289](https://github.com/influxdata/influxdb-java/issues/289) Batching enhancements: Pending asynchronous writes can be explicitly flushed via `InfluxDB.flush()`.
- Add a listener to notify asynchronous errors during batch flushes [PR #318](https://github.com/influxdata/influxdb-java/pull/318).

### Fixes

- [Issue #263](https://github.com/influxdata/influxdb-java/issues/263) Add databaseExists method to InfluxDB interface.

### Improvements

- Update retrofit from 2.1 to 2.2
- Update slf4j from 1.7.22 to 1.7.24
- Update okhttp3 from 3.5 to 3.6
- automatically adjust batch processor capacity [PR #282](https://github.com/influxdata/influxdb-java/pull/282)

## v2.5 [2016-12-05]

### Features

- Support writing by UDP protocal.
- Support gzip compress for http request body.
- Support setting thread factory for batch processor.
- Support chunking

### Fixes

- [Issue #162](https://github.com/influxdb/influxdb-java/issues/162) Write point using async batch mode with different rp will use same rp.
- [Issue #214](https://github.com/influxdb/influxdb-java/issues/214) Send multiple queries in one query statement will get only one result.  
- Write can't be always async if batch is enabled.

### Improvements

- Remove the limit for database name: not contain '-'.
- Support creating influxdb instance without username and password.
- Add time related util methods for converting influxdb timestamp or unix epoch time.
- correct exception type when disable batch twice.

## v2.4 [2016-10-24]

### Features

- now uses okhttp3 and retrofit2. As a result, you can now pass an OkHttpClient.Builder to the InfluxDBFactory.connect if you wish to add more interceptors, etc, to OkHttp.
- in InfluxDB 1.0.0, some queries now require a POST instead of GET. There is a flag on Query that allow this to be specified (default is still GET).

## v2.2 [2016-04-11]

### Features

- Allow writing of pre-constructed line protocol strings

### Fixes

- Correct escaping of database names for create and delete database actions
- Many bug fixes / improvements in general

### Other

- Deprecated `field()` method in preference for `addField()` methods.

## v2.1 [2015-12-05]

### Features

- Extensions to fluent builder classes
- Convenience methods for building Points
- Allow integer types as field values

### Fixes

- Fixed escaping of tag and field values
- Always uses nanosecond precision for time
- Uses NumberFormat class for safer formatting of large numbers.

## v2.0 [2015-07-17]

### Features

- Compatible with InfluxDB version 0.9+
- Support for lineprotocol
- Batched write support

## v1.4 [2014-11-11] / v1.5 [2014-11-14]

No major functional changes or improvements. Mainly library updates and code structure

## v1.3 [2014-10-22]

### Features

- Compatible with InfluxDB Version up to 0.8
- API: add a InfluxDB#createDatabase(DatabaseConfiguration) to be able to create a new Database with ShardSpaces defined.
- API: introduction of InfluxDB#createShardSpare, InfluxDB#getShardSpace and InfluxDB#dropShardSpace
- API: deprecated InfluxDB#createShard, InfluxDB#getShards and InfluxDB#dropShard, this is replaced with shardSpaces in InfluxDB >= 0.8.0
- API: renamed InfluxDB#deletePoints to InfluxDB#deleteSeries because this is what it actually does.
- [Issue #14] update docker-java for tests to 0.10.0
- Update retrofit from 1.6.0 -> 1.6.1
- Use ms instead of m for millisecond timeprecision.

## v1.2 [2014-06-28]

### Features

- [Issue #2](https://github.com/influxdb/influxdb-java/issues/2) Implement the last missing api calls ( interfaces, sync, forceCompaction, servers, shards)
- use (http://square.github.io/okhttp/, okhttp) instead of java builtin httpconnection to get failover for the http endpoint.

### Tasks

- [Issue #8](https://github.com/influxdb/influxdb-java/issues/8) Use com.github.docker-java which replaces com.kpelykh for Integration tests.
- [Issue #6](https://github.com/influxdb/influxdb-java/issues/6) Update Retrofit to 1.6.0
- [Issue #7](https://github.com/influxdb/influxdb-java/issues/7) Update Guava to 17.0
- fix dependency to guava.

## v1.1 [2014-05-31]

### Features

- Add InfluxDB#version() to get the InfluxDB Server version information.
- Changed InfluxDB#createDatabase() to match [Issue #489](https://github.com/influxdb/influxdb/issues/489) without replicationFactor.
- Updated Retrofit from 1.5.0 -> 1.5.1

## v1.0 [2014-05-6]

- Initial Release

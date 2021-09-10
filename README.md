# influxdb-java

[![Build Status](https://github.com/influxdata/influxdb-java/workflows/master/badge.svg)](https://github.com/influxdata/influxdb-java/actions)
[![codecov.io](http://codecov.io/github/influxdata/influxdb-java/coverage.svg?branch=master)](http://codecov.io/github/influxdata/influxdb-java?branch=master)
[![Issue Count](https://codeclimate.com/github/influxdata/influxdb-java/badges/issue_count.svg)](https://codeclimate.com/github/influxdata/influxdb-java)

This is the official (and community-maintained) Java client library for [InfluxDB](https://www.influxdata.com/products/influxdb-overview/) (1.x), the open source time series database that is part of the TICK (Telegraf, InfluxDB, Chronograf, Kapacitor) stack.

_Note: This library is for use with InfluxDB 1.x and [2.x compatibility API](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/). For full supports of InfluxDB 2.x features, please use the [influxdb-client-java](https://github.com/influxdata/influxdb-client-java) client._

## Adding the library to your project

The library artifact is published in Maven central, available at [https://search.maven.org/artifact/org.influxdb/influxdb-java](https://search.maven.org/artifact/org.influxdb/influxdb-java).

### Release versions

Maven dependency:

```xml
<dependency>
  <groupId>org.influxdb</groupId>
  <artifactId>influxdb-java</artifactId>
  <version>${influxdbClient.version}</version>
</dependency>
```

Gradle dependency:

```bash
compile group: 'org.influxdb', name: 'influxdb-java', version: "${influxdbClientVersion}"
```

## Features

* Querying data using:
  * [Influx Query Language (InfluxQL)](https://docs.influxdata.com/influxdb/v1.7/query_language/), with support for [bind parameters](https://docs.influxdata.com/influxdb/v1.7/tools/api/#bind-parameters) (similar to [JDBC PreparedStatement parameters](https://docs.oracle.com/javase/tutorial/jdbc/basics/prepared.html#supply_values_ps));
  * it's own [QueryBuilder](https://github.com/influxdata/influxdb-java/blob/master/QUERY_BUILDER.md), as you would do with e.g. EclipseLink or Hibernate;
  * Message Pack (requires InfluxDB [1.4+](https://www.influxdata.com/blog/whats-new-influxdb-oss-1-4/));
* Writing data using:
  * Data Point (an object provided by this library that represents a ... data point);
  * Your own POJO (you need to add a few Java Annotations);
  * [InfluxDB line protocol](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/) (for the braves only);
  * UDP, as [supported by InfluxDB](https://docs.influxdata.com/influxdb/v1.7/supported_protocols/udp/);
* Support synchronous and asynchronous writes;
* Batch support configurable with `jitter` interval, `buffer` size and `flush` interval.

## Quick start

```Java
// Create an object to handle the communication with InfluxDB.
// (best practice tip: reuse the 'influxDB' instance when possible)
final String serverURL = "http://127.0.0.1:8086", username = "root", password = "root";
final InfluxDB influxDB = InfluxDBFactory.connect(serverURL, username, password);

// Create a database...
// https://docs.influxdata.com/influxdb/v1.7/query_language/database_management/
String databaseName = "NOAA_water_database";
influxDB.query(new Query("CREATE DATABASE " + databaseName));
influxDB.setDatabase(databaseName);

// ... and a retention policy, if necessary.
// https://docs.influxdata.com/influxdb/v1.7/query_language/database_management/
String retentionPolicyName = "one_day_only";
influxDB.query(new Query("CREATE RETENTION POLICY " + retentionPolicyName
        + " ON " + databaseName + " DURATION 1d REPLICATION 1 DEFAULT"));
influxDB.setRetentionPolicy(retentionPolicyName);

// Enable batch writes to get better performance.
influxDB.enableBatch(
    BatchOptions.DEFAULTS
      .threadFactory(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        return thread;
      })
);

// Close it if your application is terminating or you are not using it anymore.
Runtime.getRuntime().addShutdownHook(new Thread(influxDB::close));

// Write points to InfluxDB.
influxDB.write(Point.measurement("h2o_feet")
    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    .tag("location", "santa_monica")
    .addField("level description", "below 3 feet")
    .addField("water_level", 2.064d)
    .build());

influxDB.write(Point.measurement("h2o_feet")
    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    .tag("location", "coyote_creek")
    .addField("level description", "between 6 and 9 feet")
    .addField("water_level", 8.12d)
    .build());

// Wait a few seconds in order to let the InfluxDB client
// write your points asynchronously (note: you can adjust the
// internal time interval if you need via 'enableBatch' call).
Thread.sleep(5_000L);

// Query your data using InfluxQL.
// https://docs.influxdata.com/influxdb/v1.7/query_language/data_exploration/#the-basic-select-statement
QueryResult queryResult = influxDB.query(new Query("SELECT * FROM h2o_feet"));

System.out.println(queryResult);
// It will print something like:
// QueryResult [results=[Result [series=[Series [name=h2o_feet, tags=null,
//      columns=[time, level description, location, water_level],
//      values=[
//         [2020-03-22T20:50:12.929Z, below 3 feet, santa_monica, 2.064],
//         [2020-03-22T20:50:12.929Z, between 6 and 9 feet, coyote_creek, 8.12]
//      ]]], error=null]], error=null]
```

## Contribute

For version change history have a look at [ChangeLog](https://github.com/influxdata/influxdb-java/blob/master/CHANGELOG.md).

### Build Requirements

* Java 1.8+
* Maven 3.5+
* Docker (for Unit testing)

Then you can build influxdb-java with all tests with:

```bash
$> export INFLUXDB_IP=127.0.0.1

$> mvn clean install

```

There is a shell script running InfluxDB and Maven from inside a Docker container and you can execute it by running:

```bash
$> ./compile-and-test.sh
```

## Useful links

* [Manual](MANUAL.md) (main documentation);
* [InfluxDB Object Mapper](INFLUXDB_MAPPER.md);
* [Query Builder](QUERY_BUILDER.md);
* [FAQ](FAQ.md);
* [Changelog](CHANGELOG.md).

## License

```license
The MIT License (MIT)

Copyright (c) 2014 Stefan Majer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

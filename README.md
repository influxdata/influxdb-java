influxdb-java
=============

[![Build Status](https://travis-ci.org/influxdata/influxdb-java.svg?branch=master)](https://travis-ci.org/influxdata/influxdb-java)
[![codecov.io](http://codecov.io/github/influxdata/influxdb-java/coverage.svg?branch=master)](http://codecov.io/github/influxdata/influxdb-java?branch=master)
[![Issue Count](https://codeclimate.com/github/influxdata/influxdb-java/badges/issue_count.svg)](https://codeclimate.com/github/influxdata/influxdb-java)

This is the Java Client library which is only compatible with InfluxDB 0.9 and higher. Maintained by [@majst01](https://github.com/majst01).

To connect to InfluxDB 0.8.x you need to use influxdb-java version 1.6.

This implementation is meant as a Java rewrite of the influxdb-go package.
All low level REST Api calls are available.

## Usages 

### Basic Usages:

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
String dbName = "aTimeSeries";
influxDB.createDatabase(dbName);

BatchPoints batchPoints = BatchPoints
				.database(dbName)
				.tag("async", "true")
				.retentionPolicy("autogen")
				.consistency(ConsistencyLevel.ALL)
				.build();
Point point1 = Point.measurement("cpu")
					.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					.addField("idle", 90L)
					.addField("user", 9L)
					.addField("system", 1L)
					.build();
Point point2 = Point.measurement("disk")
					.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					.addField("used", 80L)
					.addField("free", 1L)
					.build();
batchPoints.point(point1);
batchPoints.point(point2);
influxDB.write(batchPoints);
Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query);
influxDB.deleteDatabase(dbName);
```
Note : If you are using influxdb < 1.0.0, you should use 'default' instead of 'autogen'

If your application produces only single Points, you can enable the batching functionality of influxdb-java:

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
String dbName = "aTimeSeries";
influxDB.createDatabase(dbName);

// Flush every 2000 Points, at least every 100ms
influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);

Point point1 = Point.measurement("cpu")
					.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					.addField("idle", 90L)
					.addField("user", 9L)
					.addField("system", 1L)
					.build();
Point point2 = Point.measurement("disk")
					.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					.addField("used", 80L)
					.addField("free", 1L)
					.build();

influxDB.write(dbName, "autogen", point1);
influxDB.write(dbName, "autogen", point2);
Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query);
influxDB.deleteDatabase(dbName);
```
Note that the batching functionality creates an internal thread pool that needs to be shutdown explicitly as part of a gracefull application shut-down, or the application will not shut down properly. To do so simply call: ```influxDB.close()```

### Advanced Usages:

#### Gzip's support (version 2.5+ required):

influxdb-java client doesn't enable gzip compress for http request body by default. If you want to enable gzip to reduce transfter data's size , you can call: 
```
influxDB.enableGzip()
```

#### UDP's support (version 2.5+ required):

influxdb-java client support udp protocol now. you can call followed methods directly to write through UDP.
```
public void write(final int udpPort, final String records);
public void write(final int udpPort, final List<String> records);
public void write(final int udpPort, final Point point);
```
note: make sure write content's total size should not > UDP protocol's limit(64K), or you should use http instead of udp.

### Other Usages:
For additional usage examples have a look at [InfluxDBTest.java](https://github.com/influxdb/influxdb-java/blob/master/src/test/java/org/influxdb/InfluxDBTest.java "InfluxDBTest.java")

## Version

The latest version for maven dependence:
```
<dependency>
  <groupId>org.influxdb</groupId>
  <artifactId>influxdb-java</artifactId>
  <version>2.5</version>
</dependency>
```
For version change history have a look at [ChangeLog](https://github.com/influxdata/influxdb-java/blob/master/CHANGELOG.md).


### Build Requirements

* Java 1.7+
* Maven 3.0+
* Docker daemon running

Then you can build influxdb-java with all tests with:

    $ mvn clean install

If you don't have Docker running locally, you can skip tests with -DskipTests flag set to true:

    $ mvn clean install -DskipTests=true

If you have Docker running, but it is not at localhost (e.g. you are on a Mac and using `docker-machine`) you can set an optional environment variable `INFLUXDB_IP` to point to the correct IP address:

    $ export INFLUXDB_IP=192.168.99.100
    $ mvn test

For convenience we provide a small shell script which starts a influxdb server locally and executes `mvn clean install` with all tests inside docker containers.

```
$ ./compile-and-test.sh
```


### Publishing

This is a
[link](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide)
to the sonatype oss guide to publishing. I'll update this section once
the [jira ticket](https://issues.sonatype.org/browse/OSSRH-9728) is
closed and I'm able to upload artifacts to the sonatype repositories.

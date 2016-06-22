influxdb-java
=============

This is the Java Client library which is only compatible with InfluxDB 0.9 and higher. Maintained by [@majst01](https://github.com/majst01).

To connect to InfluxDB 0.8.x you need to use influxdb-java version 1.6.

This implementation is meant as a Java rewrite of the influxdb-go package.
All low level REST Api calls are available.

Typical usage looks like:

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
String dbName = "aTimeSeries";
influxDB.createDatabase(dbName);

BatchPoints batchPoints = BatchPoints
				.database(dbName)
				.tag("async", "true")
				.retentionPolicy("default")
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

influxDB.write(dbName, "default", point1);
influxDB.write(dbName, "default", point2);
Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query);
influxDB.deleteDatabase(dbName);
```




### Maven
```
		<dependency>
			<groupId>org.influxdb</groupId>
			<artifactId>influxdb-java</artifactId>
			<version>2.2</version>
		</dependency>
```


For additional usage examples have a look at [InfluxDBTest.java](https://github.com/influxdb/influxdb-java/blob/master/src/test/java/org/influxdb/InfluxDBTest.java "InfluxDBTest.java")

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

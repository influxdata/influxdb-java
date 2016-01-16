influxdb-java
=============

This is the Java Client library which is only compatible with InfluxDB 0.9 and higher. 

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
					.field("idle", 90L).field("system", 9L)
					.field("system", 1L)
					.build();
Point point2 = Point.measurement("disk")
					.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					.field("used", 80L)
					.field("free", 1L)
					.build();
batchPoints.point(point1);
batchPoints.point(point2);
influxDB.write(batchPoints);
Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query);
influxDB.deleteDatabase(dbName)
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
					.field("idle", 90L).field("system", 9L)
					.field("system", 1L)
					.build();
Point point2 = Point.measurement("disk")
					.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					.field("used", 80L)
					.field("free", 1L)
					.build();

influxDB.write(dbName, "default", point1);
influxDB.write(dbName, "default", point2);
Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query);
influxDB.deleteDatabase(dbName)
```


Both query and write requests may also be performed asynchronously by passing a callback:

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
                    .field("idle", 90L).field("system", 9L)
                    .field("system", 1L)
                    .build();
Point point2 = Point.measurement("disk")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .field("used", 80L)
                    .field("free", 1L)
                    .build();
batchPoints.point(point1);
batchPoints.point(point2);


influxDB.write(batchPoints, new Callback<Void>() {
			
	public void success(Void writeResult, Response response) {
		Query query = new Query("SELECT idle FROM cpu", dbName);
		influxDB.query(query, new Callback<QueryResult>() {
			
			public void success(QueryResult queryResult, Response response) {
				System.out.println(queryResult);
				influxDB.deleteDatabase(dbName);
			}
			
			public void failure(RetrofitError error) { /* do something ... */ }
		});
	}
	
	public void failure(RetrofitError error) { /* do something ... */ }
});
```

### Maven
```
		<dependency>
			<groupId>org.influxdb</groupId>
			<artifactId>influxdb-java</artifactId>
			<version>2.1</version>
		</dependency>
```


For additional usage examples have a look at [InfluxDBTest.java](https://github.com/influxdb/influxdb-java/blob/master/src/test/java/org/influxdb/InfluxDBTest.java "InfluxDBTest.java")

### Build Requirements

* Java 1.7+
* Maven 3.0+
* Docker daemon running

Maven will run tests during build process using a docker image with influxdb actual image is majst01/influxdb-java.
This docker image is pulled during the first test run which will take some time. So the first test execution will fail because the image to pull is not there.
You can check with:

```
    $ docker images | grep majst01
majst01/influxdb-java      latest              50256afac0c9        About an hour ago   298.7 MB

```

Then you can build influxdb-java with all tests with:

    $ mvn clean install

If you don't have Docker running locally, you can skip tests with -DskipTests flag set to true:

    $ mvn clean install -DskipTests=true



### Publishing

This is a
[link](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide)
to the sonatype oss guide to publishing. I'll update this section once
the [jira ticket](https://issues.sonatype.org/browse/OSSRH-9728) is
closed and I'm able to upload artifacts to the sonatype repositories.

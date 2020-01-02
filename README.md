# influxdb-java

[![Build Status](https://travis-ci.org/influxdata/influxdb-java.svg?branch=master)](https://travis-ci.org/influxdata/influxdb-java)
[![codecov.io](http://codecov.io/github/influxdata/influxdb-java/coverage.svg?branch=master)](http://codecov.io/github/influxdata/influxdb-java?branch=master)
[![Issue Count](https://codeclimate.com/github/influxdata/influxdb-java/badges/issue_count.svg)](https://codeclimate.com/github/influxdata/influxdb-java)

Note: This library is for use with InfluxDB 1.x. For connecting to InfluxDB 2.x instances, please use the [influxdb-client-java](https://github.com/influxdata/influxdb-client-java) client.

This is the Java Client library which is only compatible with InfluxDB 0.9 and higher. Maintained by [@majst01](https://github.com/majst01).

To connect to InfluxDB 0.8.x you need to use influxdb-java version 1.6.

This implementation is meant as a Java rewrite of the influxdb-go package.
All low level REST Api calls are available.

## Usage

### Basic Usage

This is a recommended approach to write data points into InfluxDB. The influxdb-java
client is storing your writes into an internal buffer and flushes them asynchronously
to InfluxDB at a fixed flush interval to achieve good performance on both client and
server side. This requires influxdb-java v2.7 or newer.

If you want to write data points immediately into InfluxDB and synchronously process
resulting errors see [this section.](#synchronous-writes)

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
String dbName = "aTimeSeries";
influxDB.query(new Query("CREATE DATABASE " + dbName));
influxDB.setDatabase(dbName);
String rpName = "aRetentionPolicy";
influxDB.query(new Query("CREATE RETENTION POLICY " + rpName + " ON " + dbName + " DURATION 30h REPLICATION 2 SHARD DURATION 30m DEFAULT"));
influxDB.setRetentionPolicy(rpName);

influxDB.enableBatch(BatchOptions.DEFAULTS);

influxDB.write(Point.measurement("cpu")
    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    .addField("idle", 90L)
    .addField("user", 9L)
    .addField("system", 1L)
    .build());

influxDB.write(Point.measurement("disk")
    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    .addField("used", 80L)
    .addField("free", 1L)
    .build());

Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query);
influxDB.query(new Query("DROP RETENTION POLICY " + rpName + " ON " + dbName));
influxDB.query(new Query("DROP DATABASE " + dbName));
influxDB.close();
```

Any errors that happen during the batch flush won't leak into the caller of the `write` method. By default, any kind of errors will be just logged with "SEVERE" level.
If you need to be notified and do some custom logic when such asynchronous errors happen, you can add an error handler with a `BiConsumer<Iterable<Point>, Throwable>` using the overloaded `enableBatch` method:

```java
influxDB.enableBatch(BatchOptions.DEFAULTS.exceptionHandler(
        (failedPoints, throwable) -> { /* custom error handling here */ })
);
```

With batching enabled the client provides two strategies how to deal with errors thrown by the InfluxDB server.

   1. 'One shot' write - on failed write request to InfluxDB server an error is reported to the client using the means mentioned above.
   2. 'Retry on error' write (used by default) - on failed write the request by the client is repeated after batchInterval elapses
       (if there is a chance the write will succeed - the error was caused by overloading the server, a network error etc.)
       When new data points are written before the previous (failed) points are successfully written, those are queued inside the client
       and wait until older data points are successfully written.
       Size of this queue is limited and configured by `BatchOptions.bufferLimit` property. When the limit is reached, the oldest points
       in the queue are dropped. 'Retry on error' strategy is used when individual write batch size defined by `BatchOptions.actions` is lower than `BatchOptions.bufferLimit`.

Note:

* Batching functionality creates an internal thread pool that needs to be shutdown explicitly as part of a graceful application shut-down, or the application will not shut down properly. To do so simply call: ```influxDB.close()```
* `InfluxDB.enableBatch(BatchOptions)` is available since version 2.9. Prior versions use `InfluxDB.enableBatch(actions, flushInterval, timeUnit)` or similar based on the configuration parameters you want to set.
* APIs to create and drop retention policies are supported only in versions > 2.7
* If you are using influxdb < 2.8, you should use retention policy: 'autogen'
* If you are using influxdb < 1.0.0, you should use 'default' instead of 'autogen'

If your points are written into different databases and retention policies, the more complex InfluxDB.write() methods can be used:

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
String dbName = "aTimeSeries";
influxDB.query(new Query("CREATE DATABASE " + dbName));
String rpName = "aRetentionPolicy";
influxDB.query(new Query("CREATE RETENTION POLICY " + rpName + " ON " + dbName + " DURATION 30h REPLICATION 2 SHARD DURATION 30m DEFAULT"));

// Flush every 2000 Points, at least every 100ms
influxDB.enableBatch(BatchOptions.DEFAULTS.actions(2000).flushDuration(100));

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

influxDB.write(dbName, rpName, point1);
influxDB.write(dbName, rpName, point2);
Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query);
influxDB.query(new Query("DROP RETENTION POLICY " + rpName + " ON " + dbName));
influxDB.query(new Query("DROP DATABASE " + dbName));
influxDB.close();
```

#### Synchronous writes

If you want to write the data points immediately to InfluxDB (and handle the errors as well) without any delays see the following example:

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
String dbName = "aTimeSeries";
influxDB.query(new Query("CREATE DATABASE " + dbName));
String rpName = "aRetentionPolicy";
influxDB.query(new Query("CREATE RETENTION POLICY " + rpName + " ON " + dbName + " DURATION 30h REPLICATION 2 DEFAULT"));

BatchPoints batchPoints = BatchPoints
                .database(dbName)
                .tag("async", "true")
                .retentionPolicy(rpName)
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
influxDB.query(new Query("DROP RETENTION POLICY " + rpName + " ON " + dbName));
influxDB.query(new Query("DROP DATABASE " + dbName));
```

#### Try-with-resources
Try-with-resources is a new feature in JDK7, InfluxDB support this feature, you can use it just like the following example:

```java
try (InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root")) {
        // Read or Write, do any thing you want
}
```

#### Default database to query

If you only use one database you can set client level database information. This database will be used for all subsequent HTTP queries, 
but if you still sometimes need to query some different database you can, you need to provide provide database information directly in the query,
then database information in the query will take precedence and query will be pushed to that database. 
This is shown in the following example:

```java
// Database names
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");

String db1 = "database1";
influxDB.query(new Query("CREATE DATABASE " + db1));
String defaultRpName = "aRetentionPolicy1";
influxDB.query(new Query("CREATE RETENTION POLICY " + rp1 + " ON " + db1 + " DURATION 30h REPLICATION 2 DEFAULT"));
Point point1 = Point.measurement("cpu")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("idle", 90L)
                    .addField("user", 9L)
                    .addField("system", 1L)
                    .build();
influxDB.write(db1, rp1, point1); // Write to db1

String db2 = "database2";
influxDB.query(new Query("CREATE DATABASE " + db2));
String rp2 = "aRetentionPolicy1";
influxDB.query(new Query("CREATE RETENTION POLICY " + rp2 + " ON " + db2 + " DURATION 30h REPLICATION 2 DEFAULT"));
Point point2 = Point.measurement("cpu")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("idle", 80L)
                    .addField("user", 8L)
                    .addField("system", 2L)
                    .build();
influxDB.write(db2, rp2, point2); // Write to db2


influxDB.query(new Query("SELECT * FROM cpu"));     // Returns Point1
influxDB.query(new Query("SELECT * FROM cpu", db2)) // Returns Point2

```  


### Advanced Usage

#### Gzip's support (version 2.5+ required)

influxdb-java client doesn't enable gzip compress for http request body by default. If you want to enable gzip to reduce transfer data's size , you can call:

```java
influxDB.enableGzip()
```

#### UDP's support (version 2.5+ required)

influxdb-java client support udp protocol now. you can call following methods directly to write through UDP.

```java
public void write(final int udpPort, final String records);
public void write(final int udpPort, final List<String> records);
public void write(final int udpPort, final Point point);
```

Note: make sure write content's total size should not > UDP protocol's limit(64K), or you should use http instead of udp.

#### Chunking support (version 2.6+ required)

influxdb-java client now supports influxdb chunking. The following example uses a chunkSize of 20 and invokes the specified Consumer (e.g. System.out.println) for each received QueryResult

```java
Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query, 20, queryResult -> System.out.println(queryResult));
```

#### QueryResult mapper to POJO (version 2.7+ required, version 2.17+ for @TimeColumn annotation required)

An alternative way to handle the QueryResult object is now available.
Supposing that you have a measurement _CPU_:

```sql
> INSERT cpu,host=serverA,region=us_west idle=0.64,happydevop=false,uptimesecs=123456789i
>
> select * from cpu
name: cpu
time                           happydevop host    idle region  uptimesecs
----                           ---------- ----    ---- ------  ----------
2017-06-20T15:32:46.202829088Z false      serverA 0.64 us_west 123456789
```

And the following tag keys:

```sql
> show tag keys from cpu
name: cpu
tagKey
------
host
region
```

1. Create a POJO to represent your measurement. For example:

```Java
public class Cpu {
    private Instant time;
    private String hostname;
    private String region;
    private Double idle;
    private Boolean happydevop;
    private Long uptimeSecs;
    // getters (and setters if you need)
}
```

2. Add @Measurement,@TimeColumn and @Column annotations:

```Java
@Measurement(name = "cpu")
public class Cpu {
    @TimeColumn
    @Column(name = "time")
    private Instant time;
    @Column(name = "host", tag = true)
    private String hostname;
    @Column(name = "region", tag = true)
    private String region;
    @Column(name = "idle")
    private Double idle;
    @Column(name = "happydevop")
    private Boolean happydevop;
    @Column(name = "uptimesecs")
    private Long uptimeSecs;
    // getters (and setters if you need)
}
```

3. Call _InfluxDBResultMapper.toPOJO(...)_ to map the QueryResult to your POJO:

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
String dbName = "myTimeseries";
QueryResult queryResult = influxDB.query(new Query("SELECT * FROM cpu", dbName));

InfluxDBResultMapper resultMapper = new InfluxDBResultMapper(); // thread-safe - can be reused
List<Cpu> cpuList = resultMapper.toPOJO(queryResult, Cpu.class);
```

#### Writing using POJO
The same way we use `annotations` to transform data to POJO, we can write data as POJO.
Having the same POJO class Cpu

```java
String dbName = "myTimeseries";
String rpName = "aRetentionPolicy";
// Cpu has annotations @Measurement,@TimeColumn and @Column
Cpu cpu = new Cpu();
// ... setting data

Point point = Point.measurementByPOJO(cpu.getClass()).addFieldsFromPOJO(cpu).build();

influxDB.write(dbName, rpName, point);
```

#### QueryResult mapper limitations

* If your InfluxDB query contains multiple SELECT clauses, you will have to call InfluxResultMapper#toPOJO() multiple times to map every measurement returned by QueryResult to the respective POJO;
* If your InfluxDB query contains multiple SELECT clauses **for the same measurement**, InfluxResultMapper will process all results because there is no way to distinguish which one should be mapped to your POJO. It may result in an invalid collection being returned;
* A Class field annotated with _@Column(..., tag = true)_ (i.e. a [InfluxDB Tag](https://docs.influxdata.com/influxdb/v1.2/concepts/glossary/#tag-value)) must be declared as _String_.

#### QueryBuilder:

An alternative way to create InfluxDB queries is available. By using the [QueryBuilder](QUERY_BUILDER.md) you can create queries using java instead of providing the influxdb queries as strings.

### InfluxDBMapper

In case you want to save and load data using models you can use the [InfluxDBMapper](INFLUXDB_MAPPER.md).

#### Query using Callbacks (version 2.8+ required)

influxdb-java now supports returning results of a query via callbacks. Only one
of the following consumers are going to be called once :

```java
this.influxDB.query(new Query("SELECT idle FROM cpu", dbName), queryResult -> {
    // Do something with the result...
}, throwable -> {
    // Do something with the error...
});
```

#### Query using parameter binding ("prepared statements", version 2.10+ required)

If your Query is based on user input, it is good practice to use parameter binding to avoid [injection attacks](https://en.wikipedia.org/wiki/SQL_injection).
You can create queries with parameter binding with the help of the QueryBuilder:

```java
Query query = QueryBuilder.newQuery("SELECT * FROM cpu WHERE idle > $idle AND system > $system")
        .forDatabase(dbName)
        .bind("idle", 90)
        .bind("system", 5)
        .create();
QueryResult results = influxDB.query(query);
```

The values of the bind() calls are bound to the placeholders in the query ($idle, $system).

#### Batch flush interval jittering (version 2.9+ required)

When using large number of influxdb-java clients against a single server it may happen that all the clients
will submit their buffered points at the same time and possibly overloading the server. This is usually happening
when all the clients are started at once - for instance as members of cloud hosted large cluster networks.  
If all the clients have the same flushDuration set this situation will repeat periodically.

To solve this situation the influxdb-java offers an option to offset the flushDuration by a random interval so that
the clients will flush their buffers in different intervals:

```java
influxDB.enableBatch(BatchOptions.DEFAULTS.jitterDuration(500));
```

### Other Usages

For additional usage examples have a look at [InfluxDBTest.java](https://github.com/influxdb/influxdb-java/blob/master/src/test/java/org/influxdb/InfluxDBTest.java "InfluxDBTest.java")

## Version

The latest version for maven dependence:

```xml
<dependency>
  <groupId>org.influxdb</groupId>
  <artifactId>influxdb-java</artifactId>
  <version>2.17</version>
</dependency>
```

Or when using with gradle:

```groovy
compile 'org.influxdb:influxdb-java:2.17'
```

For version change history have a look at [ChangeLog](https://github.com/influxdata/influxdb-java/blob/master/CHANGELOG.md).

### Build Requirements

* Java 1.8+ (tested with jdk8 and jdk11)
* Maven 3.0+ (tested with maven 3.5.0)
* Docker daemon running

Then you can build influxdb-java with all tests with:

```bash
mvn clean install
```

If you don't have Docker running locally, you can skip tests with `-DskipTests` flag:

```bash
mvn clean install -DskipTests
```

If you have Docker running, but it is not at localhost (e.g. you are on a Mac and using `docker-machine`) you can set an optional environment variable `INFLUXDB_IP` to point to the correct IP address:

```bash
export INFLUXDB_IP=192.168.99.100
mvn test
```

For convenience we provide a small shell script which starts a influxdb server locally and executes `mvn clean install` with all tests inside docker containers.

```bash
./compile-and-test.sh
```

### Publishing

This is a
[link](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide)
to the sonatype oss guide to publishing. I'll update this section once
the [jira ticket](https://issues.sonatype.org/browse/OSSRH-9728) is
closed and I'm able to upload artifacts to the sonatype repositories.

### Frequently Asked Questions

This is a [FAQ](FAQ.md) list for influxdb-java.

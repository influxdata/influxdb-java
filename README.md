influxdb-java
=============

[![Build Status](https://travis-ci.org/influxdata/influxdb-java.svg?branch=master)](https://travis-ci.org/influxdata/influxdb-java)
[![codecov.io](http://codecov.io/github/influxdata/influxdb-java/coverage.svg?branch=master)](http://codecov.io/github/influxdata/influxdb-java?branch=master)
[![Issue Count](https://codeclimate.com/github/influxdata/influxdb-java/badges/issue_count.svg)](https://codeclimate.com/github/influxdata/influxdb-java)

This is the Java Client library which is only compatible with InfluxDB 0.9 and higher. Maintained by [@majst01](https://github.com/majst01).

To connect to InfluxDB 0.8.x you need to use influxdb-java version 1.6.

This implementation is meant as a Java rewrite of the influxdb-go package.
All low level REST Api calls are available.

## Usage

### Basic Usage:
 
This is a recommended approach to write data points into InfluxDB. The influxdb-java 
client is storing your writes into an internal buffer and flushes them asynchronously 
to InfluxDB at a fixed flush interval to achieve good performance on both client and 
server side. This requires influxdb-java v2.7 or newer.

If you want to write data points immediately into InfluxDB and synchronously process
resulting errors see [this section.](#synchronous-writes)

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
String dbName = "aTimeSeries";
influxDB.createDatabase(dbName);
influxDB.setDatabase(dbName);
String rpName = "aRetentionPolicy";
influxDB.createRetentionPolicy(rpName, dbName, "30d", "30m", 2, true);
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
influxDB.dropRetentionPolicy(rpName, dbName);
influxDB.deleteDatabase(dbName);
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
influxDB.createDatabase(dbName);
String rpName = "aRetentionPolicy";
influxDB.createRetentionPolicy(rpName, dbName, "30d", "30m", 2, true);

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
influxDB.dropRetentionPolicy(rpName, dbName);
influxDB.deleteDatabase(dbName);
influxDB.close();
```
 

#### Synchronous writes

If you want to write the data points immediately to InfluxDB (and handle the errors as well) without any delays see the following example: 

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
String dbName = "aTimeSeries";
influxDB.createDatabase(dbName);
String rpName = "aRetentionPolicy";
influxDB.createRetentionPolicy(rpName, dbName, "30d", "30m", 2, true);

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
influxDB.dropRetentionPolicy(rpName, dbName);
influxDB.deleteDatabase(dbName);
```

### Advanced Usage:

#### Gzip's support (version 2.5+ required):

influxdb-java client doesn't enable gzip compress for http request body by default. If you want to enable gzip to reduce transfer data's size , you can call:
```java
influxDB.enableGzip()
```

#### UDP's support (version 2.5+ required):

influxdb-java client support udp protocol now. you can call following methods directly to write through UDP.
```java
public void write(final int udpPort, final String records);
public void write(final int udpPort, final List<String> records);
public void write(final int udpPort, final Point point);
```
note: make sure write content's total size should not > UDP protocol's limit(64K), or you should use http instead of udp.


#### Chunking support (version 2.6+ required):

influxdb-java client now supports influxdb chunking. The following example uses a chunkSize of 20 and invokes the specified Consumer (e.g. System.out.println) for each received QueryResult
```java
Query query = new Query("SELECT idle FROM cpu", dbName);
influxDB.query(query, 20, queryResult -> System.out.println(queryResult));
```

#### QueryResult mapper to POJO (version 2.7+ required):

An alternative way to handle the QueryResult object is now available.
Supposing that you have a measurement _CPU_:
```
> INSERT cpu,host=serverA,region=us_west idle=0.64,happydevop=false,uptimesecs=123456789i
>
> select * from cpu
name: cpu
time                           happydevop host    idle region  uptimesecs
----                           ---------- ----    ---- ------  ----------
2017-06-20T15:32:46.202829088Z false      serverA 0.64 us_west 123456789
```
And the following tag keys:
```
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
2. Add @Measurement and @Column annotations:
```Java
@Measurement(name = "cpu")
public class Cpu {
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
```
InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
String dbName = "myTimeseries";
QueryResult queryResult = influxDB.query(new Query("SELECT * FROM cpu", dbName));

InfluxDBResultMapper resultMapper = new InfluxDBResultMapper(); // thread-safe - can be reused
List<Cpu> cpuList = resultMapper.toPOJO(queryResult, Cpu.class);
```
**QueryResult mapper limitations**
- If your InfluxDB query contains multiple SELECT clauses, you will have to call InfluxResultMapper#toPOJO() multiple times to map every measurement returned by QueryResult to the respective POJO;
- If your InfluxDB query contains multiple SELECT clauses **for the same measurement**, InfluxResultMapper will process all results because there is no way to distinguish which one should be mapped to your POJO. It may result in an invalid collection being returned;
- A Class field annotated with _@Column(..., tag = true)_ (i.e. a [InfluxDB Tag](https://docs.influxdata.com/influxdb/v1.2/concepts/glossary/#tag-value)) must be declared as _String_.
-- _Note: With the current released version (2.7), InfluxDBResultMapper does not support QueryResult created by queries using the "GROUP BY" clause. This was fixed by [PR #345](https://github.com/influxdata/influxdb-java/pull/345)._

#### QueryBuilder:
An alternative way to create InfluxDB queries is available.

Supposing that you have a measurement _h2o_feet_:
```
> SELECT * FROM "h2o_feet"

name: h2o_feet
--------------
time                   level description      location       water_level
2015-08-18T00:00:00Z   below 3 feet           santa_monica   2.064
2015-08-18T00:00:00Z   between 6 and 9 feet   coyote_creek   8.12
[...]
2015-09-18T21:36:00Z   between 3 and 6 feet   santa_monica   5.066
2015-09-18T21:42:00Z   between 3 and 6 feet   santa_monica   4.938
```

Issue simple select statements

```java
Query query = select().from(DATABASE,"h2o_feet");
```

```sqlite-psql
SELECT * FROM "h2o_feet"
```

Select specific tags and fields from a single measurement

```java
Query query = select("level description","location","water_level").from(DATABASE,"h2o_feet");
```


```sqlite-psql
SELECT "level description",location,water_level FROM h2o_feet;
```

Select specific tags and fields from a single measurement, and provide their identifier type


```java
Query query = select().column("\"level description\"::field").column("\"location\"::tag").column("\"water_level\"::field").from(DATABASE,"h2o_feet");
```


```sqlite-psql
SELECT "level description"::field,"location"::tag,"water_level"::field FROM h2o_feet;
```

Select all fields from a single measurement

```java
Query query = select().raw("*::field").from(DATABASE,"h2o_feet");
```


```sqlite-psql
SELECT *::field FROM h2o_feet;
```

Select a specific field from a measurement and perform basic arithmetic


```java
Query query = select().op(op(cop("water_level",MUL,2),"+",4)).from(DATABASE,"h2o_feet");
```


```sqlite-psql
SELECT (water_level * 2) + 4 FROM h2o_feet;
```

Select all data from more than one measurement

```java
Query query = select().from(DATABASE,"\"h2o_feet\",\"h2o_pH\"");
```


```sqlite-psql
SELECT * FROM "h2o_feet","h2o_pH";
```

Select all data from a fully qualified measurement

```java
Query query = select().from(DATABASE,"\"NOAA_water_database\".\"autogen\".\"h2o_feet\"");
```

```sqlite-psql
SELECT * FROM "NOAA_water_database"."autogen"."h2o_feet";
```

Select data that have specific field key-values

```java
Query query = select().from(DATABASE,"h2o_feet").where(gt("water_level",8));
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE water_level > 8;
```

Select data that have a specific string field key-value

```java
Query query = select().from(DATABASE,"h2o_feet").where(eq("level description","below 3 feet"));
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE "level description" = 'below 3 feet';
```

Select data that have a specific field key-value and perform basic arithmetic

```java
Query query = select().from(DATABASE,TABLE).where(gt(cop("water_level",ADD,2),11.9));
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE (water_level + 2) > 11.9;
```

Select data that have a specific tag key-value

```java
Query query = select().column("water_level").from(DATABASE,TABLE).where(eq("location","santa_monica"));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica';
```

Select data that have specific field key-values and tag key-values

```java
Query query = select().column("water_level").from(DATABASE,TABLE)
                .where(neq("location","santa_monica"))
                .andNested()
                .and(lt("water_level",-0.59))
                .or(gt("water_level",9.95))
                .close();
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95);
```

Select data that have specific timestamps

```java
Query query = select().from(DATABASE,TABLE)
                .where(gt("time",subTime(7,DAY)));
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE time > now() - 7d;
```

Group query results by a single tag

```java
Query query = select().mean("water_level").from(DATABASE,TABLE) .groupBy("location");
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet GROUP BY location;
```

Group query results by more than one tag

```java
Query query = select().mean("index").from(DATABASE,TABLE)
                .groupBy("location","randtag");
```

```sqlite-psql
SELECT MEAN(index) FROM h2o_feet GROUP BY location,randtag;
```

Group query results by all tags

```java
Query query = select().mean("index").from(DATABASE,TABLE)
                .groupBy(raw("*"));
```

```sqlite-psql
SELECT MEAN(index) FROM h2o_feet GROUP BY *;
```

**GROUP BY time intervals**

Group query results into 12 minute intervals

```java
Query query = select().count("water_level").from(DATABASE,TABLE)
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:30:00Z'"))
                .groupBy(time(12l,MINUTE));
```

```sqlite-psql
SELECT COUNT(water_level) FROM h2o_feet WHERE location = 'coyote_creek' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z'' GROUP BY time(12m);
```

Group query results into 12 minutes intervals and by a tag key

```java
        Query query = select().count("water_level").from(DATABASE,TABLE)
                .where()
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:30:00Z'"))
                .groupBy(time(12l,MINUTE),"location");
```

```sqlite-psql
SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z'' GROUP BY time(12m),location;
```

***Advanced GROUP BY time() syntax***

Group query results into 18 minute intervals and shift the preset time boundaries forward

```java
Query query = select().mean("water_level").from(DATABASE,TABLE)
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:06:00Z"))
                .and(lte("time","2015-08-18T00:54:00Z"))
                .groupBy(time(18l,MINUTE,6l,MINUTE));
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE location = 'coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m,6m);
```

Group query results into 12 minute intervals and shift the preset time boundaries back

```java
Query query = select().mean("water_level").from(DATABASE,TABLE)
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:06:00Z"))
                .and(lte("time","2015-08-18T00:54:00Z"))
                .groupBy(time(18l,MINUTE,-12l,MINUTE));
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE location = 'coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m,-12m);
```



```java

```

```sqlite-psql

```

***The INTO clause***

Rename a database

```java
Query select =
                select()
                        .into("\"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT")
                        .from(DATABASE, "\"NOAA_water_database\".\"autogen\"./.*/")
                        .groupBy(new RawText("*"));
```

```sqlite-psql
SELECT * INTO "copy_NOAA_water_database"."autogen".:MEASUREMENT FROM "NOAA_water_database"."autogen"./.*/ GROUP BY *;
```

Write the results of a query to a measurement

```java
Query select = select().column("water_level").into("h2o_feet_copy_1").from(DATABASE,TABLE).where(eq("location","coyote_creek"));
```

```sqlite-psql
SELECT water_level INTO h2o_feet_copy_1 FROM h2o_feet WHERE location = 'coyote_creek';
```

Write aggregated results to a measurement

```java
Query select = select()
                .mean("water_level")
                .into("all_my_averages")
                .from(DATABASE,TABLE)
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:30:00Z"))
                .groupBy(time(12l,MINUTE));
```

```sqlite-psql
SELECT MEAN(water_level) INTO all_my_averages FROM h2o_feet WHERE location = 'coyote_creek' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m);
```

Write aggregated results for more than one measurement to a different database (downsampling with backreferencing)

```java

```

```sqlite-psql

```

***ORDER BY time DESC***

Return the newest points first

```java
Query select = select().from(DATABASE,TABLE)
                .where(eq("location","santa_monica"))
                .orderBy(desc());
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE location = 'santa_monica' ORDER BY time DESC;
```

Return the newest points first and include a GROUP BY time() clause

```java
Query select = select().mean("water_level")
                .from(DATABASE,TABLE)
                .where(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:42:00Z"))
                .groupBy(time(12l,MINUTE))
                .orderBy(desc());
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY time(12m) ORDER BY time DESC;
```

***The LIMIT clause***

Limit the number of points returned

```java
Query select = select("water_level","location")
                .from(DATABASE,TABLE).limit(3);
```

```sqlite-psql
SELECT water_level,location FROM h2o_feet LIMIT 3;
```

Limit the number points returned and include a GROUP BY clause

```java
Query select = select().mean("water_level")
                .from(DATABASE,TABLE)
                .where()
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:42:00Z"))
                .groupBy(raw("*"),time(12l,MINUTE))
                .limit(2);
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) LIMIT 2;
```

***The SLIMIT clause***

Limit the number of series returned

```java
Query select = select().column("water_level")
                .from(DATABASE,"h2o_fleet")
                .groupBy(raw("*"))
                .sLimit(1);
```

```sqlite-psql
SELECT water_level FROM "h2o_feet" GROUP BY * SLIMIT 1

```
Limit the number of series returned and include a GROUP BY time() clause

```java
Query select = select().column("water_level")
                .from(DATABASE,TABLE)
                .where()
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:42:00Z"))
                .groupBy(raw("*"),time(12l,MINUTE))
                .sLimit(1);
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) SLIMIT 1;
```

***The OFFSET clause***

Paginate points

```java
Query select = select("water_level","location").from(DATABASE,TABLE).limit(3,3);
```

```sqlite-psql
SELECT water_level,location FROM h2o_feet LIMIT 3 OFFSET 3;
```

***The SOFFSET clause***

Paginate series and include all clauses

```java
Query select = select().mean("water_level")
                .from(DATABASE,TABLE)
                .where()
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:42:00Z"))
                .groupBy(raw("*"),time(12l,MINUTE))
                .orderBy(desc())
                .limit(2,2)
                .sLimit(1,1);
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) ORDER BY time DESC LIMIT 2 OFFSET 2 SLIMIT 1 SOFFSET 1;
```

***The Time Zone clause***

Return the UTC offset for Chicago’s time zone

```java
Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .groupBy("test2", "test3")
            .sLimit(1)
            .tz("America/Chicago");
```

```sqlite-psql
SELECT test1 FROM foobar GROUP BY test2,test3 SLIMIT 1 tz('America/Chicago');
```

***Time Syntax***

Specify a time range with RFC3339 date-time strings

```java
Query select = select().column("water_level")
                .from(DATABASE,TABLE)
                .where(eq("location","santa_monica"))
                .and(gte("time","2015-08-18T00:00:00.000000000Z"))
                .and(lte("time","2015-08-18T00:12:00Z"));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= '2015-08-18T00:00:00.000000000Z' AND time <= '2015-08-18T00:12:00Z';
```

Specify a time range with second-precision epoch timestamps

```java
Query select = select().column("water_level")
                .from(DATABASE,TABLE)
                .where(eq("location","santa_monica"))
                .and(gte("time",ti(1439856000l,SECOND)))
                .and(lte("time",ti(1439856720l,SECOND)));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= 1439856000s AND time <= 1439856720s;
```

Perform basic arithmetic on an RFC3339-like date-time string

```java
Query select = select().column("water_level")
                .from(DATABASE,TABLE)
                .where(eq("location","santa_monica"))
                .and(gte("time",op("2015-09-18T21:24:00Z",SUB,ti(6l,MINUTE))));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= '2015-09-18T21:24:00Z' - 6m;
```

Perform basic arithmetic on an epoch timestamp

```java
Query select = select().column("water_level")
                .from(DATABASE,TABLE)
                .where(eq("location","santa_monica"))
                .and(gte("time",op(ti(24043524l,MINUTE),SUB,ti(6l,MINUTE))));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= 24043524m - 6m;
```

Specify a time range with relative time

```java
Query select = select().column("water_level")
                .from(DATABASE,TABLE)
                .where(eq("location","santa_monica"))
                .and(gte("time",subTime(1l,HOUR)));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= now() - 1h;
```

***Regular expressions***

Use a regular expression to specify field keys and tag keys in the SELECT clause


```java
Query select = select().regex("l").from(DATABASE,TABLE).limit(1);
```

```sqlite-psql
SELECT /l/ FROM h2o_feet LIMIT 1;
```

Use a regular expression to specify field keys with a function in the SELECT clause

```java
Query select = select().regex("l").distinct().from(DATABASE,TABLE).limit(1);
```

```sqlite-psql
SELECT DISTINCT /l/ FROM h2o_feet LIMIT 1;
```

Use a regular expression to specify a field value in the WHERE clause

```java
Query select = select().regex("/l/").from(DATABASE,TABLE).where(regex("level description","/between/")).limit(1);
```

```sqlite-psql
SELECT /l/ FROM h2o_feet WHERE "level description" =~ /between/ LIMIT 1;
```

Use a regular expression to specify tag keys in the GROUP BY clause

```java
Query select = select().regex("/l/").from(DATABASE,TABLE).where(regex("level description","/between/")).groupBy(raw("/l/")).limit(1);
```

```sqlite-psql
SELECT /l/ FROM h2o_feet WHERE "level description" =~ /between/ GROUP BY /l/ LIMIT 1;
```

Function with no direct implementation can be supported by raw expressions

```java
Query select = select().raw("an expression on select").from(dbName, "cpu").where("an expression as condition");
```

```sqlite-psql
SELECT an expression on select FROM h2o_feet WHERE an expression as condition;
```

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
influxDB.enableBatch(BatchOptions.DEFAULTS.jitterDuration(500);
```

### Other Usages:
For additional usage examples have a look at [InfluxDBTest.java](https://github.com/influxdb/influxdb-java/blob/master/src/test/java/org/influxdb/InfluxDBTest.java "InfluxDBTest.java")

## Version

The latest version for maven dependence:
```xml
<dependency>
  <groupId>org.influxdb</groupId>
  <artifactId>influxdb-java</artifactId>
  <version>2.10</version>
</dependency>
```
Or when using with gradle:
```groovy
compile 'org.influxdb:influxdb-java:2.10'
```
For version change history have a look at [ChangeLog](https://github.com/influxdata/influxdb-java/blob/master/CHANGELOG.md).


### Build Requirements

* Java 1.8+ (tested with jdk8 and jdk9)
* Maven 3.0+ (tested with maven 3.5.0)
* Docker daemon running

Then you can build influxdb-java with all tests with:

```bash
$ mvn clean install
```

If you don't have Docker running locally, you can skip tests with -DskipTests flag set to true:

```bash
$ mvn clean install -DskipTests=true
```

If you have Docker running, but it is not at localhost (e.g. you are on a Mac and using `docker-machine`) you can set an optional environment variable `INFLUXDB_IP` to point to the correct IP address:

```bash
$ export INFLUXDB_IP=192.168.99.100
$ mvn test
```

For convenience we provide a small shell script which starts a influxdb server locally and executes `mvn clean install` with all tests inside docker containers.

```bash
$ ./compile-and-test.sh
```


### Publishing

This is a
[link](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide)
to the sonatype oss guide to publishing. I'll update this section once
the [jira ticket](https://issues.sonatype.org/browse/OSSRH-9728) is
closed and I'm able to upload artifacts to the sonatype repositories.

### Frequently Asked Questions

This is a [FAQ](FAQ.md) list for influxdb-java.


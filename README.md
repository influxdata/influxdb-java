influxdb-java
=============

A pure Java library to access the REST API of a InfluxDB database.

This implementation is meant as a Java rewrite of the influxdb-go package.
All low level REST Api calls are available.

Typical usage looks like:

```java
InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");

this.influxDB.createDatabase("aTimeSeries", 1);

Serie serie = new Serie("testSeries");
serie.setColumns(new String[] { "value1", "value2" });
Object[] point = new Object[] { System.currentTimeMillis(), 5 };
serie.setPoints(new Object[][] { point });
Serie[] series = new Serie[] { serie };
this.influxDB.write(dbName, series, TimeUnit.MILLISECONDS);

```

### TODO

Publish to maven-repo.

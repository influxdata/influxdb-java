package org.influxdb;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * Test the InfluxDB API over MessagePack format
 *
 * @author hoan.le [at] bonitoo.io
 *
 */
@RunWith(JUnitPlatform.class)
@EnabledIfEnvironmentVariable(named = "INFLUXDB_VERSION", matches = "1\\.6|1\\.5|1\\.4")
public class MessagePackInfluxDBTest extends InfluxDBTest {
  /**
   * Create a influxDB connection before all tests start.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Override
  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    influxDB = TestUtils.connectToInfluxDB(ResponseFormat.MSGPACK);
    influxDB.createDatabase(UDP_DATABASE);
  }
  
  /**
   * Tests writing points using the time precision feature
   * @throws Exception
   */
  @Override
  @Test
  public void testWriteBatchWithPrecision() throws Exception {
    // GIVEN a database and a measurement
    String dbName = "precision_unittest_" + System.currentTimeMillis();
    this.influxDB.createDatabase(dbName);

    String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

    String measurement = TestUtils.getRandomMeasurement();

    long t1 = 1485273600;
    Point p1 = Point
        .measurement(measurement)
        .addField("foo", 1d)
        .tag("device", "one")
        .time(t1, TimeUnit.SECONDS).build(); // 2017-01-27T16:00:00

    long t2 = 1485277200;
    Point p2 = Point
        .measurement(measurement)
        .addField("foo", 2d)
        .tag("device", "two")
        .time(t2, TimeUnit.SECONDS).build(); // 2017-01-27T17:00:00

    long t3 = 1485280800;
    Point p3 = Point
        .measurement(measurement)
        .addField("foo", 3d)
        .tag("device", "three")
        .time(t3, TimeUnit.SECONDS).build(); // 2017-01-27T18:00:00

    BatchPoints batchPoints = BatchPoints
        .database(dbName)
        .retentionPolicy(rp)
        .precision(TimeUnit.SECONDS)
        .points(p1, p2, p3)
        .build();

    // WHEN I write the batch
    this.influxDB.write(batchPoints);

    // THEN the measure points have a timestamp with second precision
    QueryResult queryResult = this.influxDB.query(new Query("SELECT * FROM " + measurement, dbName));
    long bySecond = TimeUnit.NANOSECONDS.toSeconds(
        (Long) queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(0));
    Assertions.assertEquals(bySecond, t1);
    
    bySecond = TimeUnit.NANOSECONDS.toSeconds(
        (Long) queryResult.getResults().get(0).getSeries().get(0).getValues().get(1).get(0));
    Assertions.assertEquals(bySecond, t2);
    
    bySecond = TimeUnit.NANOSECONDS.toSeconds(
        (Long) queryResult.getResults().get(0).getSeries().get(0).getValues().get(2).get(0));
    Assertions.assertEquals(bySecond, t3);

    this.influxDB.deleteDatabase(dbName);
  }

  @Override
  @Test
  public void testWriteBatchWithoutPrecision() throws Exception {
    // GIVEN a database and a measurement
    String dbName = "precision_unittest_" + System.currentTimeMillis();
    this.influxDB.createDatabase(dbName);

    String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

    String measurement = TestUtils.getRandomMeasurement();

    // GIVEN a batch of points that has no specific precision
    long t1 = 1485273600000000100L;
    Point p1 = Point
        .measurement(measurement)
        .addField("foo", 1d)
        .tag("device", "one")
        .time(t1, TimeUnit.NANOSECONDS).build(); // 2017-01-27T16:00:00.000000100Z
    Double timeP1 = Double.valueOf(t1);

    long t2 = 1485277200000000200L;
    Point p2 = Point
        .measurement(measurement)
        .addField("foo", 2d)
        .tag("device", "two")
        .time(t2, TimeUnit.NANOSECONDS).build(); // 2017-01-27T17:00:00.000000200Z
    Double timeP2 = Double.valueOf(t2);

    long t3 = 1485280800000000300L;
    Point p3 = Point
        .measurement(measurement)
        .addField("foo", 3d)
        .tag("device", "three")
        .time(t3, TimeUnit.NANOSECONDS).build(); // 2017-01-27T18:00:00.000000300Z
    Double timeP3 = Double.valueOf(t3);

    BatchPoints batchPoints = BatchPoints
        .database(dbName)
        .retentionPolicy(rp)
        .points(p1, p2, p3)
        .build();

    // WHEN I write the batch
    this.influxDB.write(batchPoints);

    // THEN the measure points have a timestamp with second precision
    QueryResult queryResult = this.influxDB.query(new Query("SELECT * FROM " + measurement, dbName), TimeUnit.NANOSECONDS);
    Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().size(), 3);
    Double value = Double.valueOf(queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(0).toString());
    Assertions.assertEquals(value, timeP1);
    value = Double.valueOf(queryResult.getResults().get(0).getSeries().get(0).getValues().get(1).get(0).toString());
    Assertions.assertEquals(value, timeP2);
    value = Double.valueOf(queryResult.getResults().get(0).getSeries().get(0).getValues().get(2).get(0).toString());
    Assertions.assertEquals(value, timeP3);

    this.influxDB.deleteDatabase(dbName);
  }
  
  @Override
  @Test
  public void testWriteRecordsWithPrecision() throws Exception {
    // GIVEN a database and a measurement
    String dbName = "precision_unittest_" + System.currentTimeMillis();
    this.influxDB.createDatabase(dbName);

    String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

    String measurement = TestUtils.getRandomMeasurement();
    List<String> records = new ArrayList<>();
    records.add(measurement + ",atag=test1 idle=100,usertime=10,system=1 1485273600");
    long timeP1 = 1485273600;

    records.add(measurement + ",atag=test2 idle=200,usertime=20,system=2 1485277200");
    long timeP2 = 1485277200;

    records.add(measurement + ",atag=test3 idle=300,usertime=30,system=3 1485280800");
    long timeP3 = 1485280800;

    // WHEN I write the batch
    this.influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, TimeUnit.SECONDS, records);

    // THEN the measure points have a timestamp with second precision
    QueryResult queryResult = this.influxDB.query(new Query("SELECT * FROM " + measurement, dbName));
    Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().size(), 3);
    
    long bySecond = TimeUnit.NANOSECONDS.toSeconds(
        (Long) queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(0));
    Assertions.assertEquals(bySecond, timeP1);

    bySecond = TimeUnit.NANOSECONDS.toSeconds(
        (Long) queryResult.getResults().get(0).getSeries().get(0).getValues().get(1).get(0));
    Assertions.assertEquals(bySecond, timeP2);
    
    bySecond = TimeUnit.NANOSECONDS.toSeconds(
        (Long) queryResult.getResults().get(0).getSeries().get(0).getValues().get(2).get(0));
    Assertions.assertEquals(bySecond, timeP3);

    this.influxDB.deleteDatabase(dbName);
  }

  @Override
  @Test
  public void testChunking() throws InterruptedException {
    if (this.influxDB.version().startsWith("0.") || this.influxDB.version().startsWith("1.0")) {
      // do not test version 0.13 and 1.0
      return;
    }
    String dbName = "write_unittest_" + System.currentTimeMillis();
    this.influxDB.createDatabase(dbName);
    String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
    BatchPoints batchPoints = BatchPoints.database(dbName).retentionPolicy(rp).build();
    Point point1 = Point.measurement("disk").tag("atag", "a").addField("used", 60L).addField("free", 1L).build();
    Point point2 = Point.measurement("disk").tag("atag", "b").addField("used", 70L).addField("free", 2L).build();
    Point point3 = Point.measurement("disk").tag("atag", "c").addField("used", 80L).addField("free", 3L).build();
    batchPoints.point(point1);
    batchPoints.point(point2);
    batchPoints.point(point3);
    this.influxDB.write(batchPoints);

    Thread.sleep(2000);
    final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>();
    Query query = new Query("SELECT * FROM disk", dbName);
    this.influxDB.query(query, 2, new Consumer<QueryResult>() {
      @Override
      public void accept(QueryResult result) {
        queue.add(result);
      }});

    Thread.sleep(2000);
    this.influxDB.deleteDatabase(dbName);

    QueryResult result = queue.poll(20, TimeUnit.SECONDS);
    Assertions.assertNotNull(result);
    System.out.println(result);
    Assertions.assertEquals(2, result.getResults().get(0).getSeries().get(0).getValues().size());

    result = queue.poll(20, TimeUnit.SECONDS);
    Assertions.assertNotNull(result);
    System.out.println(result);
    Assertions.assertEquals(1, result.getResults().get(0).getSeries().get(0).getValues().size());

    result = queue.poll(5, TimeUnit.SECONDS);
    Assertions.assertNull(result);
  }

  @Test
  public void testInfluxDBVersionChecking() throws InterruptedException, IOException {

    InfluxDB spy = spy(TestUtils.connectToInfluxDB(ResponseFormat.MSGPACK));

    doReturn("1.5.2").when(spy).version();
    spy.databaseExists("abc");
    spy.close();

    spy = spy(TestUtils.connectToInfluxDB(ResponseFormat.MSGPACK));
    doReturn("v1.6.0").when(spy).version();
    spy.databaseExists("abc");
    spy.close();

    assertThrows(UnsupportedOperationException.class, () -> {
      InfluxDB spy1 = spy(TestUtils.connectToInfluxDB(ResponseFormat.MSGPACK));
      try {
        doReturn("1.3.0").when(spy1).version();
        spy1.databaseExists("abc");
      } finally {
        spy1.close();
      }

    });

    assertThrows(UnsupportedOperationException.class, () -> {
      InfluxDB spy1 = spy(TestUtils.connectToInfluxDB(ResponseFormat.MSGPACK));
      try {
        doReturn("a.b.c").when(spy1).version();
        spy1.databaseExists("abc");
      } finally {
        spy1.close();
      }
    });

  }
}

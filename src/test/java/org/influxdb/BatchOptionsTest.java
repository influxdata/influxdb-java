package org.influxdb;

import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

@RunWith(JUnitPlatform.class)
public class BatchOptionsTest {

  private InfluxDB influxDB;

  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    this.influxDB = TestUtils.connectToInfluxDB();
  }

  /**
   * Test the implementation of {@link InfluxDB#enableBatch(int, int, TimeUnit, ThreadFactory)}.
   */
  //@Test
  public void testBatchEnabledWithDefaultSettings() {
    try {
      this.influxDB.enableBatch();

    }
    finally {
      this.influxDB.disableBatch();
    }
  }

  //@Test
  public void testParametersSet() {
    BatchOptions options = BatchOptions.DEFAULTS.actions(3);
    Assertions.assertEquals(3, options.getActions());
    options=options.consistency(InfluxDB.ConsistencyLevel.ANY);
    Assertions.assertEquals(InfluxDB.ConsistencyLevel.ANY, options.getConsistency());
    options=options.flushDuration(1001);
    Assertions.assertEquals(1001, options.getFlushDuration());
    options=options.bufferLimit(7070);
    Assertions.assertEquals(7070, options.getBufferLimit());
    options=options.jitterDuration(104);
    Assertions.assertEquals(104, options.getJitterDuration());
    BiConsumer<Iterable<Point>, Throwable> handler=new BiConsumer<Iterable<Point>, Throwable>() {
      @Override
      public void accept(Iterable<Point> points, Throwable throwable) {

      }
    };
    options=options.exceptionHandler(handler);
    Assertions.assertEquals(handler, options.getExceptionHandler());
    ThreadFactory tf=Executors.defaultThreadFactory();
    options=options.threadFactory(tf);
    Assertions.assertEquals(tf, options.getThreadFactory());
  }

  /**
   * Test the implementation of {@link BatchOptions#actions(int)} }.
   */
  //@Test
  public void testActionsSetting() throws InterruptedException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BatchOptions options = BatchOptions.DEFAULTS.actions(3);

      this.influxDB.enableBatch(options);
      this.influxDB.createDatabase(dbName);
      this.influxDB.setDatabase(dbName);
      for (int j = 0; j < 5; j++) {
        Point point = Point.measurement("cpu")
                .time(j,TimeUnit.MILLISECONDS)
                .addField("idle", (double) j)
                .addField("user", 2.0 * j)
                .addField("system", 3.0 * j).build();
        this.influxDB.write(point);
      }
      Thread.sleep(500);
      QueryResult result=influxDB.query(new Query("select * from cpu", dbName));
      Assertions.assertEquals(3, result.getResults().get(0).getSeries().get(0).getValues().size());
    }
    finally {
      this.influxDB.disableBatch();
      this.influxDB.deleteDatabase(dbName);
    }
  }

  /**
   * Test the implementation of {@link BatchOptions#flushDuration(int)} }.
   * @throws InterruptedException 
   */
  //@Test
  public void testFlushDuration() throws InterruptedException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BatchOptions options = BatchOptions.DEFAULTS.flushDuration(10000);

      this.influxDB.createDatabase(dbName);
      this.influxDB.setDatabase(dbName);
      this.influxDB.enableBatch(options);
      for (int j = 0; j < 20; j++) {
        Point point = Point.measurement("weather")
                .time(j,TimeUnit.HOURS)
                .addField("temperature", (double) j)
                .addField("humidity", (double) (j) * 1.1)
                .addField("uv_index", "moderate").build();
        this.influxDB.write(point);
      }
      
      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());
      
      Thread.sleep(12000);
      result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    }
    finally {
      this.influxDB.disableBatch();
      this.influxDB.deleteDatabase(dbName);
    }
  }
  
  /**
   * Test the implementation of {@link BatchOptions#jitterDuration(int)} }.
   * @throws InterruptedException 
   */
  //@Test
  public void testJitterDuration() throws InterruptedException {
    
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BatchOptions options = BatchOptions.DEFAULTS.flushDuration(1000).jitterDuration(125);

      this.influxDB.createDatabase(dbName);
      this.influxDB.setDatabase(dbName);
      this.influxDB.enableBatch(options);
      for (int j = 0; j < 20; j++) {
        Point point = Point.measurement("weather")
                .time(j,TimeUnit.HOURS)
                .addField("temperature", (double) j)
                .addField("humidity", (double) (j) * 1.1)
                .addField("uv_index", "moderate").build();
        this.influxDB.write(point);
      }
      
      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());
      
      Thread.sleep(1125);
      result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    }
    finally {
      this.influxDB.disableBatch();
      this.influxDB.deleteDatabase(dbName);
    }
    
    
  }
  
  /**
   * Test the implementation of {@link BatchOptions#jitterDuration(int)} }.
   */
  //@Test
  public void testNegativeJitterDuration() {
    
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      BatchOptions options = BatchOptions.DEFAULTS.jitterDuration(-10);
      influxDB.enableBatch(options);
      
      influxDB.disableBatch();
      options = BatchOptions.DEFAULTS.jitterDuration(0);
      influxDB.enableBatch();
      influxDB.disableBatch();
    });
  }
  
  /**
   * Test the implementation of {@link BatchOptions#bufferLimit(int)} }.
   */
  @Test
  public void testBufferLimit() throws InterruptedException {
    
    int[][] bufferLimit2Actions = {{10, 4}, {3, 4}};  
    
    for (int[] bufferLimit2Action : bufferLimit2Actions) {
      String dbName = "write_unittest_" + System.currentTimeMillis();
      try {
        BatchOptions options = BatchOptions.DEFAULTS.bufferLimit(bufferLimit2Action[0]).actions(bufferLimit2Action[1]);

        this.influxDB.createDatabase(dbName);
        this.influxDB.setDatabase(dbName);
        this.influxDB.enableBatch(options);
        for (int j = 0; j < 10; j++) {
          Point point = Point.measurement("weather")
                  .time(j,TimeUnit.HOURS)
                  .addField("temperature", (double) j)
                  .addField("humidity", (double) (j) * 1.1)
                  .addField("uv_index", "moderate").build();
          this.influxDB.write(point);
        }
        
        QueryResult result = influxDB.query(new Query("select * from weather", dbName));
        Assertions.assertEquals(8, result.getResults().get(0).getSeries().get(0).getValues().size());      
        Thread.sleep(1000);
        result = influxDB.query(new Query("select * from weather", dbName));
        Assertions.assertEquals(10, result.getResults().get(0).getSeries().get(0).getValues().size());
      }
      finally {
        this.influxDB.disableBatch();
        this.influxDB.deleteDatabase(dbName);
      }
    }
    
  }
  
  /**
   * Test the implementation of {@link BatchOptions#bufferLimit(int)} }.
   */
  //@Test
  public void testNegativeBufferLimit() {
    
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      BatchOptions options = BatchOptions.DEFAULTS.bufferLimit(-10);
      influxDB.enableBatch(options);
      
      influxDB.disableBatch();
      options = BatchOptions.DEFAULTS.bufferLimit(0);
      influxDB.enableBatch();
      influxDB.disableBatch();
    });
  }
}

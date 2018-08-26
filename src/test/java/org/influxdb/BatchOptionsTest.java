package org.influxdb;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;


@RunWith(JUnitPlatform.class)
public class BatchOptionsTest {

  InfluxDB influxDB;

  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    this.influxDB = TestUtils.connectToInfluxDB();
  }

  /**
   * Test the implementation of {@link InfluxDB#enableBatch(int, int, TimeUnit, ThreadFactory)}.
   */
  @Test
  public void testBatchEnabledWithDefaultSettings() {
    try {
      this.influxDB.enableBatch();

    }
    finally {
      this.influxDB.disableBatch();
    }
  }

  @Test
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
  @Test
  public void testActionsSetting() throws InterruptedException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BatchOptions options = BatchOptions.DEFAULTS.actions(3).flushDuration(100);

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

      //wait for at least one flush period
      Thread.sleep(200);
      //test at least 3 points was written
      QueryResult result = influxDB.query(new Query("select * from cpu", dbName));
      int size = result.getResults().get(0).getSeries().get(0).getValues().size();
      Assertions.assertTrue(size >= 3, "there must be be at least 3 points written");
      
      //wait for at least one flush period
      Thread.sleep(200);

      //test all 5 points was written 
      result = influxDB.query(new Query("select * from cpu", dbName));
      Assertions.assertEquals(5, result.getResults().get(0).getSeries().get(0).getValues().size());
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
  @Test
  public void testFlushDuration() throws InterruptedException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BatchOptions options = BatchOptions.DEFAULTS.flushDuration(200);
      influxDB.createDatabase(dbName);
      influxDB.setDatabase(dbName);
      influxDB.enableBatch(options);
      write20Points(influxDB);
      
      //check no points writen to DB before the flush duration
      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());
      
      //wait for at least one flush
      Thread.sleep(500);
      result = influxDB.query(new Query("select * from weather", dbName));
      
      //check points written already to DB 
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
  @Test
  public void testJitterDuration() throws InterruptedException {
   
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BatchOptions options = BatchOptions.DEFAULTS.flushDuration(100).jitterDuration(500);
      influxDB.createDatabase(dbName);
      influxDB.setDatabase(dbName);
      influxDB.enableBatch(options);
      write20Points(influxDB);
      
      Thread.sleep(100);
      
      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());
      
      //wait for at least one flush
      Thread.sleep(1000);
      result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    }
    finally {
      influxDB.disableBatch();
      influxDB.deleteDatabase(dbName);
    }
  }
  
  /**
   * Test the implementation of {@link BatchOptions#jitterDuration(int)} }.
   */
  @Test
  public void testNegativeJitterDuration() {
    
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      BatchOptions options = BatchOptions.DEFAULTS.jitterDuration(-10);
      influxDB.enableBatch(options);
      influxDB.disableBatch();
    });
  }

  /**
   * Test the implementation of {@link BatchOptions#bufferLimit(int)} }.
   */
  @Test
  public void testNegativeBufferLimit() {
    
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      BatchOptions options = BatchOptions.DEFAULTS.bufferLimit(-10);
      influxDB.enableBatch(options);
      influxDB.disableBatch();
    });
  }
  
  /**
   * Test the implementation of {@link BatchOptions#threadFactory(ThreadFactory)} }.
   * @throws InterruptedException
   */
  @Test
  public void testThreadFactory() throws InterruptedException {
    
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      ThreadFactory spy = spy(new ThreadFactory() {
        
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        @Override
        public Thread newThread(Runnable r) {
          return threadFactory.newThread(r);
        }
      });
      BatchOptions options = BatchOptions.DEFAULTS.threadFactory(spy).flushDuration(100);

      influxDB.createDatabase(dbName);
      influxDB.setDatabase(dbName);
      influxDB.enableBatch(options);
      write20Points(influxDB);
      
      Thread.sleep(500);
      //Test the thread factory is used somewhere
      verify(spy, atLeastOnce()).newThread(any());
      
      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    } finally {
      this.influxDB.disableBatch();
      this.influxDB.deleteDatabase(dbName);
    }
    
  }
  


  /**
   * Test the implementation of {@link BatchOptions#consistency(InfluxDB.ConsistencyLevel)} }.
   * @throws InterruptedException 
   * @throws IOException 
   */
  @Test
  public void testConsistency() throws InterruptedException, IOException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    final Map<String, Object> params = new HashMap<>();
    InfluxDB influxDB = createInterceptedInfluxDb(chain -> {
      HttpUrl url = chain.request().url();
      if ("/write".equals(url.encodedPath())) {
        String consistencyLevel = url.queryParameter("consistency");
        Assertions.assertEquals(params.get("consistencyLevel").toString(), consistencyLevel.toUpperCase());
      }
      return chain.proceed(chain.request());
    });
    influxDB.createDatabase(dbName);
    influxDB.setDatabase(dbName);
    try {
      int n = 0;
      for (final ConsistencyLevel consistencyLevel : ConsistencyLevel.values()) {
        params.put("consistencyLevel", consistencyLevel);
        BatchOptions options = BatchOptions.DEFAULTS.consistency(consistencyLevel).flushDuration(100);
        influxDB.enableBatch(options);
        Assertions.assertEquals(options.getConsistency(), consistencyLevel);
        
        writeSomePoints(influxDB, n, n + 4);
        n += 5;
        Thread.sleep(300);
        
        QueryResult result = influxDB.query(new Query("select * from weather", dbName));
        Assertions.assertEquals(n, result.getResults().get(0).getSeries().get(0).getValues().size());
        
        influxDB.disableBatch();
      }
      
    } finally {
      influxDB.deleteDatabase(dbName);
    }
  }
  
  void writeSomePoints(InfluxDB influxDB, String measurement, int firstIndex, int lastIndex) {
    for (int i = firstIndex; i <= lastIndex; i++) {
      Point point = Point.measurement(measurement)
              .time(i,TimeUnit.HOURS)
              .addField("field1", (double) i)
              .addField("field2", (double) (i) * 1.1)
              .addField("field3", "moderate").build();
      influxDB.write(point);
    }
  }
  
  void writeSomePoints(InfluxDB influxDB, int firstIndex, int lastIndex) {
    for (int i = firstIndex; i <= lastIndex; i++) {
      Point point = Point.measurement("weather")
              .time(i,TimeUnit.HOURS)
              .addField("temperature", (double) i)
              .addField("humidity", (double) (i) * 1.1)
              .addField("uv_index", "moderate").build();
      influxDB.write(point);
    }
  }
  
  void write20Points(InfluxDB influxDB) {
    writeSomePoints(influxDB, 0, 19);
  }
  
  void writeSomePoints(InfluxDB influxDB, int n) {
    writeSomePoints(influxDB, 0, n - 1);
  }
  
  static String createErrorBody(String errorMessage) {
    return MessageFormat.format("'{' \"error\": \"{0}\" '}'", errorMessage);
  }
  
  private InfluxDB createInterceptedInfluxDb(Interceptor interceptor) throws InterruptedException, IOException {
    OkHttpClient.Builder client = new OkHttpClient.Builder();
    client.addInterceptor(interceptor);
    return TestUtils.connectToInfluxDB(client, null, ResponseFormat.JSON);
  }
}

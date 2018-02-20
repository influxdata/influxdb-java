package org.influxdb;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBException.DatabaseNotFoundException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.text.MessageFormat;
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
   * use a bufferLimit that less than actions, then OneShotBatchWrite is used
   */
  @Test
  public void testBufferLimitLessThanActions() throws InterruptedException {
    
    TestAnswer answer = new TestAnswer() {

      InfluxDBException influxDBException = InfluxDBException.buildExceptionForErrorState(createErrorBody(InfluxDBException.CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR)); 
      @Override
      protected void check(InvocationOnMock invocation) {
        if ((Boolean) params.get("throwException")) {
          throw influxDBException;
        }
      }
    };
    
    InfluxDB spy = spy(influxDB);
    //the spied influxDB.write(BatchPoints) will always throw InfluxDBException
    doAnswer(answer).when(spy).write(any(BatchPoints.class));
    
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      answer.params.put("throwException", true);
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.bufferLimit(3).actions(4).flushDuration(100).exceptionHandler(mockHandler);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);
      write20Points(spy);
      
      Thread.sleep(300);
      verify(mockHandler, atLeastOnce()).accept(any(), any());
      
      QueryResult result = spy.query(new Query("select * from weather", dbName));
      //assert 0 point written because of InfluxDBException and OneShotBatchWriter did not retry
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());
      
      answer.params.put("throwException", false);
      write20Points(spy);
      Thread.sleep(300);
      result = spy.query(new Query("select * from weather", dbName));
      //assert all 20 points written to DB due to no exception
      Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    }
    finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }
    
  }

  /**
   * Test the implementation of {@link BatchOptions#bufferLimit(int)} }.
   * use a bufferLimit that greater than actions, then RetryCapableBatchWriter is used
   */
  @Test
  public void testBufferLimitGreaterThanActions() throws InterruptedException {
    TestAnswer answer = new TestAnswer() {
      
      int nthCall = 0;
      InfluxDBException cacheMaxMemorySizeExceededException = InfluxDBException.buildExceptionForErrorState(createErrorBody(InfluxDBException.CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR)); 
      @Override
      protected void check(InvocationOnMock invocation) {
        
        switch (nthCall++) {
        case 0:
          throw InfluxDBException.buildExceptionForErrorState(createErrorBody(InfluxDBException.DATABASE_NOT_FOUND_ERROR));
        case 1:
          throw InfluxDBException.buildExceptionForErrorState(createErrorBody(InfluxDBException.CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR));
        default:
          break;
        }
      }
    };

    InfluxDB spy = spy(influxDB);
    doAnswer(answer).when(spy).write(any(BatchPoints.class));
    
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.bufferLimit(10).actions(8).flushDuration(100).exceptionHandler(mockHandler);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);
      writeSomePoints(spy, "measurement1", 0, 5);
      
      Thread.sleep(300);
      verify(mockHandler, atLeastOnce()).accept(any(), any());
      
      QueryResult result = spy.query(new Query("select * from measurement1", dbName));
      //assert 0 point written because of non-retry capable DATABASE_NOT_FOUND_ERROR and RetryCapableBatchWriter did not retry
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());
      
      writeSomePoints(spy, "measurement2", 0, 5);
      
      Thread.sleep(300);
      
      result = spy.query(new Query("select * from measurement2", dbName));
      //assert all 6 point written because of retry capable CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR and RetryCapableBatchWriter did retry
      Assertions.assertEquals(6, result.getResults().get(0).getSeries().get(0).getValues().size());
    }
    finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }
        
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
   * Test the implementation of {@link BatchOptions#exceptionHandler(BiConsumer)} }.
   * @throws InterruptedException
   */
  @Test
  public void testHandlerOnRetryImpossible() throws InterruptedException {
    
    String dbName = "write_unittest_" + System.currentTimeMillis();
    InfluxDB spy = spy(influxDB);
    doThrow(DatabaseNotFoundException.class).when(spy).write(any(BatchPoints.class));
    
    try {
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.exceptionHandler(mockHandler).flushDuration(100);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);
      
      writeSomePoints(spy, 1);
      
      Thread.sleep(200);
      verify(mockHandler, times(1)).accept(any(), any());
      
      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());
    } finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }
    
  }
  
  /**
   * Test the implementation of {@link BatchOptions#exceptionHandler(BiConsumer)} }.
   * @throws InterruptedException
   */
  @Test
  public void testHandlerOnRetryPossible() throws InterruptedException {
    
    String dbName = "write_unittest_" + System.currentTimeMillis();
    InfluxDB spy = spy(influxDB);
    doAnswer(new Answer<Object>() {
      boolean firstCall = true;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (firstCall) {
          firstCall = false;
          throw new InfluxDBException("error");
        } else {
          return invocation.callRealMethod();
        }
      }
    }).when(spy).write(any(BatchPoints.class));
    
    try {
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.exceptionHandler(mockHandler).flushDuration(100);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);
      
      writeSomePoints(spy, 1);
      
      Thread.sleep(500);
      verify(mockHandler, never()).accept(any(), any());
      
      verify(spy, times(2)).write(any(BatchPoints.class));
      
      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertNotNull(result.getResults().get(0).getSeries());
      Assertions.assertEquals(1, result.getResults().get(0).getSeries().get(0).getValues().size());
      
    } finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }
    
  }

  /**
   * Test the implementation of {@link BatchOptions#consistency(InfluxDB.ConsistencyLevel)} }.
   * @throws InterruptedException 
   */
  @Test
  public void testConsistency() throws InterruptedException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    
    InfluxDB spy = spy(influxDB);
    spy.createDatabase(dbName);
    spy.setDatabase(dbName);
    try {
      TestAnswer answer = new TestAnswer() {
        @Override
        protected void check(InvocationOnMock invocation) {
          BatchPoints batchPoints = (BatchPoints) invocation.getArgument(0);
          Assertions.assertEquals(params.get("consistencyLevel"), batchPoints.getConsistency());
          
        }
      };
      doAnswer(answer).when(spy).write(any(BatchPoints.class));

      int n = 0;
      for (final ConsistencyLevel consistencyLevel : ConsistencyLevel.values()) {
        answer.params.put("consistencyLevel", consistencyLevel);
        BatchOptions options = BatchOptions.DEFAULTS.consistency(consistencyLevel).flushDuration(100);
        spy.enableBatch(options);
        Assertions.assertEquals(options.getConsistency(), consistencyLevel);
        
        writeSomePoints(spy, n, n + 4);
        n += 5;
        Thread.sleep(300);
        
        verify(spy, atLeastOnce()).write(any(BatchPoints.class));
        QueryResult result = spy.query(new Query("select * from weather", dbName));
        Assertions.assertEquals(n, result.getResults().get(0).getSeries().get(0).getValues().size());
        
        
        spy.disableBatch();
      }
      
    } finally {
      spy.deleteDatabase(dbName);
    }
  }
  
  private void writeSomePoints(InfluxDB influxDB, String measurement, int firstIndex, int lastIndex) {
    for (int i = firstIndex; i <= lastIndex; i++) {
      Point point = Point.measurement(measurement)
              .time(i,TimeUnit.HOURS)
              .addField("field1", (double) i)
              .addField("field2", (double) (i) * 1.1)
              .addField("field3", "moderate").build();
      influxDB.write(point);
    }
  }
  
  private void writeSomePoints(InfluxDB influxDB, int firstIndex, int lastIndex) {
    for (int i = firstIndex; i <= lastIndex; i++) {
      Point point = Point.measurement("weather")
              .time(i,TimeUnit.HOURS)
              .addField("temperature", (double) i)
              .addField("humidity", (double) (i) * 1.1)
              .addField("uv_index", "moderate").build();
      influxDB.write(point);
    }
  }
  
  private void write20Points(InfluxDB influxDB) {
    writeSomePoints(influxDB, 0, 19);
  }
  
  private void writeSomePoints(InfluxDB influxDB, int n) {
    writeSomePoints(influxDB, 0, n - 1);
  }
  
  private static String createErrorBody(String errorMessage) {
    return MessageFormat.format("'{' \"error\": \"{0}\" '}'", errorMessage);
  }
}

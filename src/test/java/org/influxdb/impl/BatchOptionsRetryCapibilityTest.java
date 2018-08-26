package org.influxdb.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.TestAnswer;
import org.influxdb.TestUtils;
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

/**
 * Test cases of BatchOptions on a failure retry-capable InfluxDB Implementation  
 *
 * @author hoan.le [at] bonitoo.io
 *
 */

@RunWith(JUnitPlatform.class)
public class BatchOptionsRetryCapibilityTest {
  
  private InfluxDB influxDB;
  
  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    this.influxDB = TestUtils.connectToInfluxDB();
  }
  /**
   * Test the implementation of {@link BatchOptions#bufferLimit(int)} }. use a
   * bufferLimit that less than actions, then OneShotBatchWrite is used
   */
  @Test
  public void testBufferLimitLessThanActions() throws InterruptedException {

    TestAnswer answer = new TestAnswer() {

      InfluxDBException influxDBException = InfluxDBException
          .buildExceptionForErrorState(createErrorBody("cache-max-memory-size exceeded"));

      @Override
      protected void check(InvocationOnMock invocation) {
        if ((Boolean) params.get("throwException")) {
          throw influxDBException;
        }
      }
    };

    InfluxDBImpl spy = spy((InfluxDBImpl) influxDB);
    // the spied influxDBImpl.write(BatchPoints, boolean) will always throw
    // InfluxDBException
    doAnswer(answer).when(spy).writeNoRetry(any(BatchPoints.class));

    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      answer.params.put("throwException", true);
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.bufferLimit(3).actions(4).flushDuration(100)
          .exceptionHandler(mockHandler);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);
      write20Points(spy);

      Thread.sleep(300);
      verify(mockHandler, atLeastOnce()).accept(any(), any());

      QueryResult result = spy.query(new Query("select * from weather", dbName));
      // assert 0 point written because of InfluxDBException and
      // OneShotBatchWriter did not retry
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());

      answer.params.put("throwException", false);
      write20Points(spy);
      Thread.sleep(300);
      result = spy.query(new Query("select * from weather", dbName));
      // assert all 20 points written to DB due to no exception
      Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    } finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }

  }

  /**
   * Test the implementation of {@link BatchOptions#bufferLimit(int)} }. use a
   * bufferLimit that greater than actions, then RetryCapableBatchWriter is used
   */
  @Test
  public void testBufferLimitGreaterThanActions() throws InterruptedException {
    TestAnswer answer = new TestAnswer() {

      int nthCall = 0;
      InfluxDBException cacheMaxMemorySizeExceededException = InfluxDBException
          .buildExceptionForErrorState(createErrorBody("cache-max-memory-size exceeded"));

      @Override
      protected void check(InvocationOnMock invocation) {

        switch (nthCall++) {
        case 0:
          throw InfluxDBException
              .buildExceptionForErrorState(createErrorBody("database not found"));
        case 1:
          throw InfluxDBException
              .buildExceptionForErrorState(createErrorBody("cache-max-memory-size exceeded"));
        default:
          break;
        }
      }
    };

    InfluxDBImpl spy = spy((InfluxDBImpl) influxDB);
    doAnswer(answer).when(spy).writeNoRetry(any(BatchPoints.class));

    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.bufferLimit(10).actions(8).flushDuration(100)
          .exceptionHandler(mockHandler);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);
      writeSomePoints(spy, "measurement1", 0, 5);

      Thread.sleep(300);
      verify(mockHandler, atLeastOnce()).accept(any(), any());

      QueryResult result = spy.query(new Query("select * from measurement1", dbName));
      // assert 0 point written because of non-retry capable
      // DATABASE_NOT_FOUND_ERROR and RetryCapableBatchWriter did not retry
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());

      writeSomePoints(spy, "measurement2", 0, 5);

      Thread.sleep(300);

      result = spy.query(new Query("select * from measurement2", dbName));
      // assert all 6 point written because of retry capable
      // CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR and RetryCapableBatchWriter did
      // retry
      Assertions.assertEquals(6, result.getResults().get(0).getSeries().get(0).getValues().size());
    } finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }

  }

  /**
   * Test the implementation of
   * {@link BatchOptions#exceptionHandler(BiConsumer)} }.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testHandlerOnRetryImpossible() throws InterruptedException {

    String dbName = "write_unittest_" + System.currentTimeMillis();
    InfluxDBImpl spy = spy((InfluxDBImpl) influxDB);
    doThrow(DatabaseNotFoundException.class).when(spy).writeNoRetry(any(BatchPoints.class));

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
   * Test the implementation of
   * {@link BatchOptions#exceptionHandler(BiConsumer)} }.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testHandlerOnRetryPossible() throws InterruptedException {

    String dbName = "write_unittest_" + System.currentTimeMillis();
    InfluxDBImpl spy = spy((InfluxDBImpl) influxDB);
    doAnswer(new Answer() {
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
    }).when(spy).writeNoRetry(any(BatchPoints.class));

    try {
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.exceptionHandler(mockHandler).flushDuration(100);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);

      writeSomePoints(spy, 1);

      Thread.sleep(500);
      verify(mockHandler, never()).accept(any(), any());

      verify(spy, times(2)).writeNoRetry(any(BatchPoints.class));

      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertNotNull(result.getResults().get(0).getSeries());
      Assertions.assertEquals(1, result.getResults().get(0).getSeries().get(0).getValues().size());

    } finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }
  }

  @Test
  public void testWriteBatchOnRetryPossible() throws InterruptedException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    InfluxDBImpl spy = spy((InfluxDBImpl) influxDB);
    doAnswer(new Answer() {
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
    }).when(spy).writeNoRetry(any(BatchPoints.class));
    try {
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.exceptionHandler(mockHandler).flushDuration(100);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);

      BatchPoints batchPoints = createBatchPoints(dbName, "m0", 200);
      spy.write(batchPoints);
      Thread.sleep(500);
      verify(mockHandler, never()).accept(any(), any());

      verify(spy, times(2)).writeNoRetry(any(BatchPoints.class));

      QueryResult result = influxDB.query(new Query("select * from m0", dbName));
      Assertions.assertNotNull(result.getResults().get(0).getSeries());
      Assertions.assertEquals(200, result.getResults().get(0).getSeries().get(0).getValues().size());

    } finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }
  }

  @Test
  public void testWriteBatchOnRetryImPossible() throws InterruptedException {

    String dbName = "write_unittest_" + System.currentTimeMillis();
    InfluxDBImpl spy = spy((InfluxDBImpl) influxDB);
    doThrow(DatabaseNotFoundException.class).when(spy).writeNoRetry(any(BatchPoints.class));

    try {
      BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
      BatchOptions options = BatchOptions.DEFAULTS.exceptionHandler(mockHandler).flushDuration(100);

      spy.createDatabase(dbName);
      spy.setDatabase(dbName);
      spy.enableBatch(options);

      BatchPoints batchPoints = createBatchPoints(dbName, "m0", 200);
      spy.write(batchPoints);
      Thread.sleep(500);
      
      verify(mockHandler, times(1)).accept(any(), any());

      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertNull(result.getResults().get(0).getSeries());
      Assertions.assertNull(result.getResults().get(0).getError());
    } finally {
      spy.disableBatch();
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

  private BatchPoints createBatchPoints(String dbName, String measurement, int n) {
    BatchPoints batchPoints = BatchPoints.database(dbName).build();
    for (int i = 1; i <= n; i++) {
      Point point = Point.measurement(measurement)
              .time(i,TimeUnit.MILLISECONDS)
              .addField("f1", (double) i)
              .addField("f2", (double) (i) * 1.1)
              .addField("f3", "f_v3").build();
      batchPoints.point(point);
    }
    
    return batchPoints;
  }

  private static String createErrorBody(String errorMessage) {
    return MessageFormat.format("'{' \"error\": \"{0}\" '}'", errorMessage);
  }
}

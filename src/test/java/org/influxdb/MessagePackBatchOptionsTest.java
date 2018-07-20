package org.influxdb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.InfluxDBException.DatabaseNotFoundException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;

/**
 * Test the InfluxDB API over MessagePack format
 *
 * @author hoan.le [at] bonitoo.io
 *
 */
@RunWith(JUnitPlatform.class)
@EnabledIfEnvironmentVariable(named = "INFLUXDB_VERSION", matches = "1\\.6|1\\.5|1\\.4")
public class MessagePackBatchOptionsTest extends BatchOptionsTest {

  @Override
  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    influxDB = TestUtils.connectToInfluxDB(ResponseFormat.MSGPACK);
  }

  /**
   * Test the implementation of {@link BatchOptions#flushDuration(int)} }.
   * 
   * @throws InterruptedException
   */
  @Override
  @Test
  public void testFlushDuration() throws InterruptedException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BatchOptions options = BatchOptions.DEFAULTS.flushDuration(200);
      influxDB.createDatabase(dbName);
      influxDB.setDatabase(dbName);
      influxDB.enableBatch(options);
      write20Points(influxDB);

      // check no points writen to DB before the flush duration
      QueryResult result = influxDB.query(new Query("select * from weather", dbName));
      List<Series> series = result.getResults().get(0).getSeries();
      Assertions.assertNull(series);
      Assertions.assertNull(result.getResults().get(0).getError());

      // wait for at least one flush
      Thread.sleep(500);
      result = influxDB.query(new Query("select * from weather", dbName));

      // check points written already to DB
      Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    } finally {
      this.influxDB.disableBatch();
      this.influxDB.deleteDatabase(dbName);
    }
  }

  /**
   * Test the implementation of {@link BatchOptions#jitterDuration(int)} }.
   * 
   * @throws InterruptedException
   */
  @Override
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
      List<Series> series = result.getResults().get(0).getSeries();
      Assertions.assertNull(series);
      Assertions.assertNull(result.getResults().get(0).getError());

      // wait for at least one flush
      Thread.sleep(1000);
      result = influxDB.query(new Query("select * from weather", dbName));
      Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    } finally {
      influxDB.disableBatch();
      influxDB.deleteDatabase(dbName);
    }
  }

  /**
   * Test the implementation of {@link BatchOptions#bufferLimit(int)} }. use a
   * bufferLimit that less than actions, then OneShotBatchWrite is used
   */
  @Override
  @Test
  public void testBufferLimitLessThanActions() throws InterruptedException {

    TestAnswer answer = new TestAnswer() {

      InfluxDBException influxDBException = InfluxDBException
          .buildExceptionForErrorState(createErrorBody(InfluxDBException.CACHE_MAX_MEMORY_SIZE_EXCEEDED_ERROR));

      @Override
      protected void check(InvocationOnMock invocation) {
        if ((Boolean) params.get("throwException")) {
          throw influxDBException;
        }
      }
    };

    InfluxDB spy = spy(influxDB);
    // the spied influxDB.write(BatchPoints) will always throw InfluxDBException
    doAnswer(answer).when(spy).write(any(BatchPoints.class));

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
      List<Series> series = result.getResults().get(0).getSeries();
      Assertions.assertNull(series);
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
   * Test the implementation of {@link BatchOptions#bufferLimit(int)} }.
   * use a bufferLimit that greater than actions, then RetryCapableBatchWriter is used
   */
  @Override
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
      List<Series> series = result.getResults().get(0).getSeries();
      Assertions.assertNull(series);
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
   * Test the implementation of {@link BatchOptions#exceptionHandler(BiConsumer)} }.
   * @throws InterruptedException
   */
  @Override
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
      List<Series> series = result.getResults().get(0).getSeries();
      Assertions.assertNull(series);
      Assertions.assertNull(result.getResults().get(0).getError());
    } finally {
      spy.disableBatch();
      spy.deleteDatabase(dbName);
    }
    
  }
}

package org.influxdb.impl;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.TestAnswer;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.*;

@RunWith(JUnitPlatform.class)
public class RetryCapableBatchWriterTest {

  BatchPoints getBP(int count) {
    BatchPoints.Builder b = BatchPoints.database("d1");
    for (int i = 0; i < count; i++) {
      b.point(Point.measurement("x1").addField("x", 1).build()).build();
    }
    return b.build();
  }

  @Test
  public void test() {
    InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
    BiConsumer errorHandler = mock(BiConsumer.class);
    RetryCapableBatchWriter rw = new RetryCapableBatchWriter(mockInfluxDB, errorHandler,
            150, 100);
    BatchPoints bp0 = getBP(5);
    BatchPoints bp1 = getBP(90);
    BatchPoints bp2 = getBP(90);
    BatchPoints bp3 = getBP(8);
    BatchPoints bp4 = getBP(100);

    Exception nonRecoverable = InfluxDBException.buildExceptionForErrorState("{ \"error\": \"database not found: cvfdgf\" }");
    Exception recoverable = InfluxDBException.buildExceptionForErrorState("{ \"error\": \"cache-max-memory-size exceeded 104/1400\" }");
    Mockito.doThrow(nonRecoverable).when(mockInfluxDB).write(bp0);
    Mockito.doThrow(recoverable).when(mockInfluxDB).write(bp1);
    Mockito.doThrow(recoverable).when(mockInfluxDB).write(bp2);
    Mockito.doThrow(recoverable).when(mockInfluxDB).write(bp3);
    // first one will fail with non-recoverable error
    rw.write(Collections.singletonList(bp0));
    // second one will fail with recoverable error
    rw.write(Collections.singletonList(bp1));
    // will fail with recoverable error again, will remove data due to buffer limit
    rw.write(Collections.singletonList(bp2));
    // will write fail with recoverable error
    rw.write(Collections.singletonList(bp3));

    ArgumentCaptor<BatchPoints> captor = ArgumentCaptor.forClass(BatchPoints.class);
    verify(mockInfluxDB, times(4)).write(captor.capture());
    final List<BatchPoints> capturedArgument1 = captor.getAllValues();
    for (BatchPoints b : capturedArgument1) {
      System.out.println("batchSize written " + b.getPoints().size());
    }

    Assert.assertEquals(capturedArgument1.get(0).getPoints().size(), 5);
    Assert.assertEquals(capturedArgument1.get(1).getPoints().size(), 90);
    Assert.assertEquals(capturedArgument1.get(2).getPoints().size(), 90);
    Assert.assertEquals(capturedArgument1.get(3).getPoints().size(), 98);

    // error handler called twice; once for first unrecoverable write, se
    verify(errorHandler, times(2)).accept(any(),any());

    // will write data that previously were not sent, will send additional data
    Mockito.reset(mockInfluxDB);
    rw.write(Collections.singletonList(bp4));

    ArgumentCaptor<BatchPoints> captor2 = ArgumentCaptor.forClass(BatchPoints.class);
    verify(mockInfluxDB, times(2)).write(captor2.capture());
    final List<BatchPoints> capturedArgument2 = captor2.getAllValues();
    for (BatchPoints b : capturedArgument2) {
      System.out.println("batchSize written " + b.getPoints().size());
    }
    Assert.assertEquals(capturedArgument2.get(0).getPoints().size(), 98);
    Assert.assertEquals(capturedArgument2.get(1).getPoints().size(), 100);

  }
  
  @Test
  public void testAllNonRecoverableExceptions() {
    
    InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
    BiConsumer errorHandler = mock(BiConsumer.class);
    RetryCapableBatchWriter rw = new RetryCapableBatchWriter(mockInfluxDB, errorHandler,
            150, 100);

    InfluxDBException nonRecoverable1 = InfluxDBException.buildExceptionForErrorState(createErrorBody("database not found: cvfdgf"));
    InfluxDBException nonRecoverable2 = InfluxDBException.buildExceptionForErrorState(createErrorBody("points beyond retention policy 'abc'"));
    InfluxDBException nonRecoverable3 = InfluxDBException.buildExceptionForErrorState(createErrorBody("unable to parse 'abc'"));
    InfluxDBException nonRecoverable4 = InfluxDBException.buildExceptionForErrorState(createErrorBody("hinted handoff queue not empty service='abc'"));
    InfluxDBException nonRecoverable5 = InfluxDBException.buildExceptionForErrorState(createErrorBody("field type conflict 'abc'"));
    InfluxDBException nonRecoverable6 = new InfluxDBException.RetryBufferOverrunException(createErrorBody("Retry BufferOverrun Exception"));
    InfluxDBException nonRecoverable7 = InfluxDBException.buildExceptionForErrorState(createErrorBody("user is not authorized to write to database"));
    
    List<InfluxDBException> exceptions = Arrays.asList(nonRecoverable1, nonRecoverable2, nonRecoverable3,
        nonRecoverable4, nonRecoverable5, nonRecoverable6, nonRecoverable7);
    int size = exceptions.size();
    doAnswer(new TestAnswer() {
      int i = 0;
      @Override
      protected void check(InvocationOnMock invocation) {
        if (i < size) {
          throw exceptions.get(i++);
        }
      }
    }).when(mockInfluxDB).write(any(BatchPoints.class));
    
    BatchPoints bp = getBP(8);
    for (int i = 0; i < size; i++) {
      rw.write(Collections.singletonList(bp));
    }
    verify(errorHandler, times(size)).accept(any(), any());;
  }
  
  @Test
  public void testClosingWriter() {
    InfluxDB mockInfluxDB = mock(InfluxDB.class);
    BiConsumer<Iterable<Point>, Throwable> errorHandler = mock(BiConsumer.class);

    BatchPoints bp5 = getBP(5);
    BatchPoints bp6 = getBP(6);
    BatchPoints bp90 = getBP(90);
    
    doAnswer(new TestAnswer() {
      int i = 0;
      @Override
      protected void check(InvocationOnMock invocation) {
        //first 4 calls
        if (i++ < 4) {
          throw InfluxDBException.buildExceptionForErrorState("cache-max-memory-size exceeded 104/1400"); 
        }
        return;
      }
    }).when(mockInfluxDB).write(any(BatchPoints.class));
    
    RetryCapableBatchWriter rw = new RetryCapableBatchWriter(mockInfluxDB, errorHandler,
        150, 100);
    
    rw.write(Collections.singletonList(bp5));
    rw.write(Collections.singletonList(bp6));
    rw.write(Collections.singletonList(bp90));
    //recoverable exception -> never errorHandler
    verify(errorHandler, never()).accept(any(), any());
    verify(mockInfluxDB, times(3)).write(any(BatchPoints.class));
    
    rw.close();
    
    ArgumentCaptor<BatchPoints> captor4Write = ArgumentCaptor.forClass(BatchPoints.class);
    ArgumentCaptor<List<Point>> captor4Accept = ArgumentCaptor.forClass(List.class);
    verify(errorHandler, times(1)).accept(captor4Accept.capture(), any());
    verify(mockInfluxDB, times(5)).write(captor4Write.capture());
    
    //bp5 and bp6 were merged and writing of the merged batch points on closing should be failed
    Assertions.assertEquals(11, captor4Accept.getValue().size());
    //bp90 was written because no more exception thrown
    Assertions.assertEquals(90, captor4Write.getAllValues().get(4).getPoints().size());
  }
  
  @Test
  public void testRetryingKeepChronologicalOrder() {
    
    BatchPoints.Builder b = BatchPoints.database("d1");
    for (int i = 0; i < 200; i++) {
      b.point(Point.measurement("x1").time(1,TimeUnit.HOURS).
          addField("x", 1).
          tag("t", "v1").build()).build();
    }
    
    BatchPoints bp1 = b.build();
    
    b = BatchPoints.database("d1");
    
    b.point(Point.measurement("x1").time(1,TimeUnit.HOURS).
        addField("x", 2).
        tag("t", "v2").build()).build();
    
    for (int i = 0; i < 199; i++) {
      b.point(Point.measurement("x1").time(2,TimeUnit.HOURS).
          addField("x", 2).
          tag("t", "v2").build()).build();
    }
    BatchPoints bp2 = b.build();
    
    InfluxDB mockInfluxDB = mock(InfluxDB.class);
    BiConsumer<Iterable<Point>, Throwable> errorHandler = mock(BiConsumer.class);
    RetryCapableBatchWriter rw = new RetryCapableBatchWriter(mockInfluxDB, errorHandler,
        450, 150);
    doAnswer(new TestAnswer() {
      int i = 0;
      @Override
      protected void check(InvocationOnMock invocation) {
        if (i++ < 1) {
          throw InfluxDBException.buildExceptionForErrorState("cache-max-memory-size exceeded 104/1400"); 
        }
        return;
      }
    }).when(mockInfluxDB).write(any(BatchPoints.class));
    
    rw.write(Collections.singletonList(bp1));
    rw.write(Collections.singletonList(bp2));
    
    ArgumentCaptor<BatchPoints> captor4Write = ArgumentCaptor.forClass(BatchPoints.class);
    verify(mockInfluxDB, times(3)).write(captor4Write.capture());
    
    //bp1 written but failed because of recoverable cache-max-memory-size error
    Assertions.assertEquals(bp1, captor4Write.getAllValues().get(0));
    //bp1 rewritten on writing of bp2 
    Assertions.assertEquals(bp1, captor4Write.getAllValues().get(1));
    //bp2 written
    Assertions.assertEquals(bp2, captor4Write.getAllValues().get(2));
    
  }
  private static String createErrorBody(String errorMessage) {
    return MessageFormat.format("'{' \"error\": \"{0}\" '}'", errorMessage);
  }
}

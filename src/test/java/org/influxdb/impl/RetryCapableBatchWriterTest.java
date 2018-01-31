package org.influxdb.impl;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
}

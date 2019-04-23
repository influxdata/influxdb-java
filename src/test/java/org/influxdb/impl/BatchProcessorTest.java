package org.influxdb.impl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.hamcrest.Matchers;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.TestUtils;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;


@RunWith(JUnitPlatform.class)
public class BatchProcessorTest {

    @Test
    public void testSchedulerExceptionHandling() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(Integer.MAX_VALUE)
            .interval(1, TimeUnit.NANOSECONDS).build();

        doThrow(new RuntimeException()).when(mockInfluxDB).write(any(BatchPoints.class));

        Point point = Point.measurement("cpu").field("6", "").build();
        BatchProcessor.HttpBatchEntry batchEntry1 = new BatchProcessor.HttpBatchEntry(point, "db1", "");
        BatchProcessor.HttpBatchEntry batchEntry2 = new BatchProcessor.HttpBatchEntry(point, "db2", "");

        batchProcessor.put(batchEntry1);
        Thread.sleep(200); // wait for scheduler

        // first try throws an exception
        verify(mockInfluxDB, times(1)).write(any(BatchPoints.class));

        batchProcessor.put(batchEntry2);
        Thread.sleep(200); // wait for scheduler
        // without try catch the 2nd time does not occur
        verify(mockInfluxDB, times(2)).write(any(BatchPoints.class));
    }

  @Test
  public void testSchedulerExceptionHandlingCallback() throws InterruptedException, IOException {
    InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
    BiConsumer<Iterable<Point>, Throwable> mockHandler = mock(BiConsumer.class);
    BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(Integer.MAX_VALUE)
        .interval(1, TimeUnit.NANOSECONDS).exceptionHandler(mockHandler).build();

    doThrow(new RuntimeException()).when(mockInfluxDB).write(any(BatchPoints.class));

    Point point = Point.measurement("cpu").field("6", "").build();
    BatchProcessor.HttpBatchEntry batchEntry1 = new BatchProcessor.HttpBatchEntry(point, "db1", "");
    BatchProcessor.HttpBatchEntry batchEntry2 = new BatchProcessor.HttpBatchEntry(point, "db2", "");

    batchProcessor.put(batchEntry1);
    Thread.sleep(200); // wait for scheduler

    verify(mockHandler, times(1)).accept(argThat(Matchers.hasItems(point, point)), any(RuntimeException.class));
  }

    @Test
    public void testBatchWriteWithDifferenctRp() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(Integer.MAX_VALUE)
            .interval(1, TimeUnit.NANOSECONDS).build();

        Point point = Point.measurement("cpu").field("6", "").build();
        BatchProcessor.HttpBatchEntry batchEntry1 = new BatchProcessor.HttpBatchEntry(point, "db1", "rp_1");
        BatchProcessor.HttpBatchEntry batchEntry2 = new BatchProcessor.HttpBatchEntry(point, "db1", "rp_2");

        batchProcessor.put(batchEntry1);
        batchProcessor.put(batchEntry2);

        Thread.sleep(200); // wait for scheduler
        // same dbname with different rp should write two batchs instead of only one.
        verify(mockInfluxDB, times(2)).write(any(BatchPoints.class));
    }

    @Test
    public void testFlushWritesBufferedPointsAndDoesNotShutdownScheduler() throws InterruptedException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB)
                .actions(Integer.MAX_VALUE)
                .interval(1, TimeUnit.NANOSECONDS).build();

        Point point = Point.measurement("test").addField("region", "a").build();
        BatchProcessor.HttpBatchEntry httpBatchEntry = new BatchProcessor.HttpBatchEntry(point, "http", "http-rp");

        batchProcessor.put(httpBatchEntry);
        Thread.sleep(100); // wait for scheduler
        // Our put should have been written
        verify(mockInfluxDB).write(any(BatchPoints.class));

        // Force a flush which should not stop the scheduler
        batchProcessor.flush();

        batchProcessor.put(httpBatchEntry);
        Thread.sleep(100); // wait for scheduler
        // Our second put should have been written if the scheduler is still running
        verify(mockInfluxDB, times(2)).write(any(BatchPoints.class));

        verifyNoMoreInteractions(mockInfluxDB);
    }

    @Test
    public void testActionsIsZero() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
          BatchProcessor.builder(mockInfluxDB).actions(0)
              .interval(1, TimeUnit.NANOSECONDS).build();
        });
    }

    @Test
    public void testIntervalIsZero() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
          BatchProcessor.builder(mockInfluxDB).actions(1)
              .interval(0, TimeUnit.NANOSECONDS).build();
        });
    }

    @Test
    public void testInfluxDBIsNull() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = null;
        Assertions.assertThrows(NullPointerException.class, () -> {
          BatchProcessor.builder(mockInfluxDB).actions(1)
              .interval(1, TimeUnit.NANOSECONDS).build();
        });
    }

    @Test
    public void testConsistencyLevelNull() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(Integer.MAX_VALUE)
                .interval(1, TimeUnit.NANOSECONDS).build();
        assertNull(batchProcessor.getConsistencyLevel());
    }

    @Test
    public void testConsistencyLevelUpdated() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(Integer.MAX_VALUE)
                .interval(1, TimeUnit.NANOSECONDS).consistencyLevel(InfluxDB.ConsistencyLevel.ANY).build();
        assertThat(batchProcessor.getConsistencyLevel(), is(equalTo(InfluxDB.ConsistencyLevel.ANY)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void precision() throws Exception {
      String dbName = "write_unittest_" + System.currentTimeMillis();
      String rpName = "somePolicy";
      BatchWriter batchWriter;
      try (InfluxDB influxDB = TestUtils.connectToInfluxDB()) {
        try {
          influxDB.createDatabase(dbName);
          influxDB.query(new Query("CREATE RETENTION POLICY " + rpName + " ON " + dbName + " DURATION 30h REPLICATION 2 DEFAULT"));

          influxDB.enableBatch(BatchOptions.DEFAULTS.actions(2000).precision(TimeUnit.SECONDS).flushDuration(100));

          BatchProcessor batchProcessor = getPrivateField(influxDB, "batchProcessor");
          BatchWriter originalBatchWriter = getPrivateField(batchProcessor, "batchWriter");
          batchWriter = Mockito.spy(originalBatchWriter);
          setPrivateField(batchProcessor, "batchWriter", batchWriter);

          Point point1 = Point.measurement("cpu")
              .time(System.currentTimeMillis() /1000, TimeUnit.SECONDS)
              .addField("idle", 90L)
              .addField("user", 9L)
              .addField("system", 1L)
              .build();

          influxDB.write(dbName, rpName, point1);

        } finally {
          influxDB.deleteDatabase(dbName);
        }
      }

      ArgumentCaptor<Collection<BatchPoints>> argument = ArgumentCaptor.forClass(Collection.class);

      verify(batchWriter, atLeastOnce()).write(argument.capture());

      for (Collection<BatchPoints> list : argument.getAllValues()) {
        for (BatchPoints p : list) {
          assertTrue(p.toString().contains("precision=SECONDS"));
          assertFalse(p.toString().contains("precision=NANOSECONDS"));
        }
      }
    }

    @SuppressWarnings("unchecked")
    static <T> T getPrivateField(final Object obj, final String name) throws Exception {
      Field field = obj.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return (T) field.get(obj);
    }

    static void setPrivateField(final Object obj, final String name, final Object value) throws Exception {
      Field field = obj.getClass().getDeclaredField(name);
      field.setAccessible(true);
      field.set(obj, value);
  }
}

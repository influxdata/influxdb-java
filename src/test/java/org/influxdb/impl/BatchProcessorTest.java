package org.influxdb.impl;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class BatchProcessorTest {

    @Test
    public void testSchedulerExceptionHandling() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB)
                .actions(Integer.MAX_VALUE)
                .consistencyLevel(InfluxDB.ConsistencyLevel.ONE)
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
    public void testBatchWriteWithDifferenctRp() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB)
            .actions(Integer.MAX_VALUE)
            .consistencyLevel(InfluxDB.ConsistencyLevel.ONE)
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
    public void testConsistencyLevelIsHonored() {
        InfluxDB.ConsistencyLevel desiredConsistencyLevel = InfluxDB.ConsistencyLevel.QUORUM;

        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB)
                .actions(Integer.MAX_VALUE)
                .consistencyLevel(desiredConsistencyLevel)
                .interval(1, TimeUnit.DAYS).build();

        Point point = Point.measurement("test").addField("region", "a").build();
        BatchProcessor.HttpBatchEntry httpBatchEntry = new BatchProcessor.HttpBatchEntry(point, "http", "http-rp");

        batchProcessor.put(httpBatchEntry);

        batchProcessor.flush();

        ArgumentCaptor<BatchPoints> batchPoints = ArgumentCaptor.forClass(BatchPoints.class);

        verify(mockInfluxDB, times(1)).write(batchPoints.capture());

        assertThat(batchPoints.getAllValues()).hasSize(1);
        assertThat(batchPoints.getValue().getConsistency()).isEqualTo(desiredConsistencyLevel);
    }

    @Test
    public void testFlushWritesBufferedPointsAndDoesNotShutdownScheduler() throws InterruptedException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB)
                .actions(Integer.MAX_VALUE)
                .consistencyLevel(InfluxDB.ConsistencyLevel.ONE)
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

    @Test(expected = IllegalArgumentException.class)
    public void testActionsIsZero() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor.builder(mockInfluxDB).actions(0).consistencyLevel(InfluxDB.ConsistencyLevel.ONE)
            .interval(1, TimeUnit.NANOSECONDS).build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testIntervalIsZero() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor.builder(mockInfluxDB).actions(1).consistencyLevel(InfluxDB.ConsistencyLevel.ONE)
            .interval(0, TimeUnit.NANOSECONDS).build();
    }
    
    @Test(expected = NullPointerException.class)
    public void testInfluxDBIsNull() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = null;
        BatchProcessor.builder(mockInfluxDB).actions(1).consistencyLevel(InfluxDB.ConsistencyLevel.ONE)
            .interval(1, TimeUnit.NANOSECONDS).build();
    }

    @Test(expected = NullPointerException.class)
    public void testConsistencyLevelIsNull() {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor.builder(mockInfluxDB).actions(1).consistencyLevel(null)
                .interval(1, TimeUnit.NANOSECONDS).build();
    }
}

package org.influxdb.impl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class BatchProcessorTest {

    @Test
    public void testSchedulerExceptionHandling() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(Integer.MAX_VALUE)
            .interval(1, TimeUnit.NANOSECONDS).build();
        try {
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
        finally {
            batchProcessor.flush();
        }
    }

    @Test
    public void testBatchWriteWithDifferenctRp() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(Integer.MAX_VALUE)
            .interval(1, TimeUnit.NANOSECONDS).build();

        try {
            Point point = Point.measurement("cpu").field("6", "").build();
            BatchProcessor.HttpBatchEntry batchEntry1 = new BatchProcessor.HttpBatchEntry(point, "db1", "rp_1");
            BatchProcessor.HttpBatchEntry batchEntry2 = new BatchProcessor.HttpBatchEntry(point, "db1", "rp_2");

            batchProcessor.put(batchEntry1);
            batchProcessor.put(batchEntry2);

            Thread.sleep(200); // wait for scheduler
            // same dbname with different rp should write two batchs instead of only one.
            verify(mockInfluxDB, times(2)).write(any(BatchPoints.class));
        }
        finally {
            batchProcessor.flush();
        }
    }

    @Test
    public void testNoBlockOnCapacityExceeded() {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(2)
            .interval(10, TimeUnit.MINUTES).build();

        try {
            Point point = Point.measurement("cpu").addField("4", "").build();
            BatchProcessor.HttpBatchEntry batchEntry1 = new BatchProcessor.HttpBatchEntry(point, "db1", "rp_1");
            BatchProcessor.HttpBatchEntry batchEntry2 = new BatchProcessor.HttpBatchEntry(point, "db1", "rp_2");
            BatchProcessor.HttpBatchEntry batchEntry3 = new BatchProcessor.HttpBatchEntry(point, "db1", "rp_3");

            assertThat(batchProcessor.offer(batchEntry1), is(true));
            assertThat(batchProcessor.offer(batchEntry2), is(true));
            assertThat(batchProcessor.offer(batchEntry3), is(false));
        }
        finally {
            batchProcessor.flush();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testActionsIsZero() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor.builder(mockInfluxDB).actions(0)
            .interval(1, TimeUnit.NANOSECONDS).build().flush();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIntervalIsZero() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor.builder(mockInfluxDB).actions(1)
            .interval(0, TimeUnit.NANOSECONDS).build().flush();
    }

    @Test(expected = NullPointerException.class)
    public void testInfluxDBIsNull() throws InterruptedException, IOException {
        InfluxDB mockInfluxDB = null;
        BatchProcessor.builder(mockInfluxDB).actions(1)
            .interval(1, TimeUnit.NANOSECONDS).build().flush();
    }
}

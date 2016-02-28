package org.influxdb.impl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.testng.annotations.Test;

public class BatchProcessorTest {

    @Test
    public void testSchedulerExceptionHandling() throws InterruptedException {
        InfluxDB mockInfluxDB = mock(InfluxDBImpl.class);
        BatchProcessor batchProcessor = BatchProcessor.builder(mockInfluxDB).actions(Integer.MAX_VALUE)
            .interval(1, TimeUnit.NANOSECONDS).build();

        doThrow(new RuntimeException()).when(mockInfluxDB).write(any(BatchPoints.class));

        Point point = Point.measurement("cpu").field("6", "").build();
        BatchProcessor.BatchEntry batchEntry1 = new BatchProcessor.BatchEntry(point, "db1", "");
        BatchProcessor.BatchEntry batchEntry2 = new BatchProcessor.BatchEntry(point, "db2", "");

        batchProcessor.put(batchEntry1);
        Thread.sleep(200); // wait for scheduler

        // first try throws an exception
        verify(mockInfluxDB, times(1)).write(any(BatchPoints.class));

        batchProcessor.put(batchEntry2);
        Thread.sleep(200); // wait for scheduler
        // without try catch the 2nd time does not occur
        verify(mockInfluxDB, times(2)).write(any(BatchPoints.class));
    }
}

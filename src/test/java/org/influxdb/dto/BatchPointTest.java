package org.influxdb.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BatchPointTest {

    @Test
    public void testEquals() throws Exception {
        // GIVEN two batchpoint objects with the same values
        Map<String, String> tags = Maps.newHashMap();
        tags.put("key", "value");

        InfluxDB.ConsistencyLevel consistencyLevel = InfluxDB.ConsistencyLevel.ANY;

        String db = "my database";

        List<Point> points = Lists.newArrayList();
        Point p = new Point();
        p.setPrecision(TimeUnit.MILLISECONDS);
        p.setMeasurement("my measurements");
        points.add(p);

        String retentionPolicy = "autogen";

        BatchPoints b1 = new BatchPoints();
        b1.setTags(tags);
        b1.setConsistency(consistencyLevel);
        b1.setDatabase(db);
        b1.setPoints(points);
        b1.setRetentionPolicy(retentionPolicy);

        BatchPoints b2 = new BatchPoints();
        b2.setTags(tags);
        b2.setConsistency(consistencyLevel);
        b2.setDatabase(db);
        b2.setPoints(points);
        b2.setRetentionPolicy(retentionPolicy);

        // WHEN I call equals on the first with the second
        boolean equals = b1.equals(b2);

        // THEN equals returns true
        assertThat(equals).isEqualTo(true);
    }

    @Test
    public void testUnEquals() throws Exception {
        // GIVEN two batchpoint objects with different values
        Map<String, String> tags1 = Maps.newHashMap();
        tags1.put("key", "value1");

        Map<String, String> tags2 = Maps.newHashMap();
        tags2.put("key", "value2");

        InfluxDB.ConsistencyLevel consistencyLevel1 = InfluxDB.ConsistencyLevel.ANY;
        InfluxDB.ConsistencyLevel consistencyLevel2 = InfluxDB.ConsistencyLevel.ALL;

        String db1 = "my database 1";
        String db2 = "my database 2";

        List<Point> points = Lists.newArrayList();
        Point p = new Point();
        p.setPrecision(TimeUnit.MILLISECONDS);
        p.setMeasurement("my measurements");
        points.add(p);

        String retentionPolicy1 = "autogen";
        String retentionPolicy2 = "default";

        BatchPoints b1 = new BatchPoints();
        b1.setTags(tags1);
        b1.setConsistency(consistencyLevel1);
        b1.setDatabase(db1);
        b1.setPoints(points);
        b1.setRetentionPolicy(retentionPolicy1);

        BatchPoints b2 = new BatchPoints();
        b2.setTags(tags2);
        b2.setConsistency(consistencyLevel2);
        b2.setDatabase(db2);
        b2.setPoints(points);
        b2.setRetentionPolicy(retentionPolicy2);

        // WHEN I call equals on the first with the second
        boolean equals = b1.equals(b2);

        // THEN equals returns true
        assertThat(equals).isEqualTo(false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPointMissingFields() {

        String dbName = "point_missing_fields_" + System.currentTimeMillis();
        BatchPoints batchPoints = BatchPoints.database(dbName).build();
        Point point = Point.measurement("cpu").build();
        batchPoints.point(point);
    }
}

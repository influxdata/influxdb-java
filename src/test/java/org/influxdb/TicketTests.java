package org.influxdb;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test the InfluxDB API.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
@Test
public class TicketTests extends AbstractTest {
	/**
	 * Test for ticket #38
	 *
	 */
	public void testTicket38() {
		String dbName = "ticket38_" + System.currentTimeMillis();
      influxDB.createDatabase(dbName);
		Point point1 = Point
				.measurement("metric")
				.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
				.field("value", 5)
				.tag("host", "host A")
				.tag("host", "host-B")
				.tag("host", "host-\"C")
				.tag("region", "region")
				.build();
      influxDB.write(dbName, "default", point1);
      influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test for ticket #39
	 *
	 */
	public void testTicket39() {
		String dbName = "ticket39_" + System.currentTimeMillis();
      influxDB.createDatabase(dbName);
		BatchPoints batchPoints = BatchPoints
				.database(dbName)
				.tag("async", "true")
				.retentionPolicy("default")
				.consistency(ConsistencyLevel.ALL)
				.build();
		Builder builder = Point.measurement("my_type");
		builder.field("my_field", "string_value");
		Point point = builder.build();
		batchPoints.point(point);
      influxDB.write(batchPoints);
      influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test for ticket #40
	 */
	public void testTicket40() {
		String dbName = "ticket40_" + System.currentTimeMillis();
      influxDB.createDatabase(dbName);
      influxDB.enableBatch(100, 100, TimeUnit.MICROSECONDS);
		for (int i = 0; i < 1000; i++) {
			Point point = Point.measurement("cpu").field("idle", 99).build();
         influxDB.write(dbName, "default", point);
		}
      influxDB.deleteDatabase(dbName);
	}

}
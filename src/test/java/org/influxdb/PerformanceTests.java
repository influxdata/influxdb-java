package org.influxdb;

import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;

public class PerformanceTests {
	private InfluxDB influxDB;
	private final static int COUNT = 1;
	private final static int POINT_COUNT = 100000;

	@BeforeClass
	public void setUp() {
		String ip = "127.0.0.1";
		this.influxDB = InfluxDBFactory.connect("http://" + ip + ":8086", "root", "root");
		this.influxDB.setLogLevel(LogLevel.NONE);
	}

	@Test
	public void writePerformance() {
		String dbName = "write_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < COUNT; i++) {

			BatchPoints batchPoints = new BatchPoints.Builder(dbName)
					.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					.tag("blubber", "bla")
					.retentionPolicy("default")
					.build();
			for (int j = 0; j < POINT_COUNT; j++) {
				Point point = new Point.Builder("cpu")
						.field("idle", j)
						.field("user", 2 * j)
						.field("system", 3 * j)
						.build();
				batchPoints.point(point);
			}
			this.influxDB.write(batchPoints);
		}
		System.out.println("Write for " + COUNT + " writes of " + POINT_COUNT + " Points took:" + watch);
		this.influxDB.deleteDatabase(dbName);
	}

	@Test
	public void writePointsPerformance() {
		String dbName = "writepoints_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < COUNT; i++) {

			BatchPoints batchPoints = new BatchPoints.Builder(dbName)
					.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					.tag("blubber", "bla")
					.retentionPolicy("default")
					.build();
			for (int j = 0; j < POINT_COUNT; j++) {
				Point point = new Point.Builder("cpu")
						.field("idle", j)
						.field("user", 2 * j)
						.field("system", 3 * j)
						.build();
				batchPoints.point(point);
			}

			this.influxDB.writePoints(batchPoints);
		}
		System.out.println("WritePoints for " + COUNT + " writes of " + POINT_COUNT + " Points took:" + watch);
		this.influxDB.deleteDatabase(dbName);
	}
}

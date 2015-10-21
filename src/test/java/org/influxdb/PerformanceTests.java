package org.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.assertj.core.util.Lists;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.Point;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;

public class PerformanceTests {
	private InfluxDB influxDB;
	private final static int COUNT = 1;
	private final static int POINT_COUNT = 100000;
	private final static int SINGLE_POINT_COUNT = 10000;

	@BeforeClass
	public void setUp() {
		String ip = "127.0.0.1";
		this.influxDB = InfluxDBFactory.connect("http://" + ip + ":8086", "root", "root");
		this.influxDB.setLogLevel(LogLevel.NONE);
	}

	@Test(threadPoolSize = 10, enabled = false)
	public void writeSinglePointPerformance() throws InterruptedException {
		String dbName = "write_" + System.currentTimeMillis();
		influxDB.createDatabase(dbName);
		influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
		Stopwatch watch = Stopwatch.createStarted();
		for (int j = 0; j < SINGLE_POINT_COUNT; j++) {
			Point point = Point.measurement("cpu").field("idle", j).field("user", 2 * j).field("system", 3 * j).build();
			influxDB.write(dbName, "default", ConsistencyLevel.ONE, point);
		}
		influxDB.disableBatch();
		System.out.println("Single Point Write for " + SINGLE_POINT_COUNT + " writes of  Points took:" + watch);
		influxDB.deleteDatabase(dbName);
	}

	@Test(enabled = false)
	public void writePerformance() {
		String dbName = "writepoints_" + System.currentTimeMillis();
		influxDB.createDatabase(dbName);

		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < COUNT; i++) {

			List<Point> points = Lists.newArrayList();
			for (int j = 0; j < POINT_COUNT; j++) {
				Point point = Point
						.measurement("cpu")
						.field("idle", j)
						.field("user", 2 * j)
						.field("system", 3 * j)
						.tag("blubber", "bla")
						.build();
				points.add(point);
			}

			influxDB.write(dbName, "default", ConsistencyLevel.ONE, points);
		}
		System.out.println("WritePoints for " + COUNT + " writes of " + POINT_COUNT + " Points took:" + watch);
		influxDB.deleteDatabase(dbName);
	}

	@Test(enabled = true)
	public void maxWritePointsPerformance() {
		String dbName = "d";
		influxDB.createDatabase(dbName);
		influxDB.enableBatch(100000, 60, TimeUnit.SECONDS);

		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < 2000000; i++) {
			Point point = Point.measurement("s").field("v", 1).build();
			influxDB.write(dbName, "default", ConsistencyLevel.ONE, point);
		}
		System.out.println("5Mio points:" + watch);
		influxDB.deleteDatabase(dbName);
	}
}

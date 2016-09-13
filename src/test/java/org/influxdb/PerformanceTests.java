package org.influxdb;

import com.google.common.base.Stopwatch;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class PerformanceTests {
	private InfluxDB influxDB;
	private final static int COUNT = 1;
	private final static int POINT_COUNT = 100000;
	private final static int SINGLE_POINT_COUNT = 10000;

	@BeforeClass
	public void setUp() {
		this.influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true), "root", "root");
		this.influxDB.setLogLevel(LogLevel.NONE);
	}

	@Test(threadPoolSize = 10, enabled = false)
	public void writeSinglePointPerformance() throws InterruptedException {
		String dbName = "write_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		this.influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
		Stopwatch watch = Stopwatch.createStarted();
		for (int j = 0; j < SINGLE_POINT_COUNT; j++) {
			Point point = Point.measurement("cpu")
					.addField("idle", (double) j)
					.addField("user", 2.0 * j)
					.addField("system", 3.0 * j).build();
			this.influxDB.write(dbName, rp, point);
		}
		this.influxDB.disableBatch();
		System.out.println("Single Point Write for " + SINGLE_POINT_COUNT + " writes of  Points took:" + watch);
		this.influxDB.deleteDatabase(dbName);
	}

	@Test(enabled = false)
	public void writePerformance() {
		String dbName = "writepoints_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < COUNT; i++) {

			BatchPoints batchPoints = BatchPoints
					.database(dbName)
					.tag("blubber", "bla")
					.retentionPolicy(rp)
					.build();
			for (int j = 0; j < POINT_COUNT; j++) {
				Point point = Point
						.measurement("cpu")
						.addField("idle", (double) j)
						.addField("user", 2.0 * j)
						.addField("system", 3.0 * j)
						.build();
				batchPoints.point(point);
			}

			this.influxDB.write(batchPoints);
		}
		System.out.println("WritePoints for " + COUNT + " writes of " + POINT_COUNT + " Points took:" + watch);
		this.influxDB.deleteDatabase(dbName);
	}

	@Test(enabled = true)
	public void maxWritePointsPerformance() {
		String dbName = "d";
		this.influxDB.createDatabase(dbName);
		this.influxDB.enableBatch(100000, 60, TimeUnit.SECONDS);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < 2000000; i++) {
			Point point = Point.measurement("s").addField("v", 1.0).build();
			this.influxDB.write(dbName, rp, point);
		}
		System.out.println("5Mio points:" + watch);
		this.influxDB.deleteDatabase(dbName);
	}
}

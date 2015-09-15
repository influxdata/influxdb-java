package org.influxdb;

import com.google.common.base.Stopwatch;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class PerformanceTests {
   private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceTests.class);

	private InfluxDB influxDB;
	private final static int COUNT = 1;
	private final static int POINT_COUNT = 100000;
	private final static int SINGLE_POINT_COUNT = 10000;

	@BeforeClass
	public void setUp() {
		String ip = "127.0.0.1";
      influxDB = InfluxDBFactory.connect("http://" + ip + ":8086", "root", "root");
      influxDB.setLogLevel(LogLevel.NONE);
	}

	@Test(threadPoolSize = 10, enabled = false)
	public void writeSinglePointPerformance() throws InterruptedException {
		String dbName = "write_" + System.currentTimeMillis();
      influxDB.createDatabase(dbName);
      influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
		Stopwatch watch = Stopwatch.createStarted();
		for (int j = 0; j < SINGLE_POINT_COUNT; j++) {
			Point point = Point.measurement("cpu").field("idle", j).field("user", 2 * j).field("system", 3 * j).build();
         influxDB.write(dbName, "default", point);
		}
      influxDB.disableBatch();

      LOGGER.info("Single Point Write for {}  writes of  Points took:{}", SINGLE_POINT_COUNT, watch);
      influxDB.deleteDatabase(dbName);
	}

	@Test(enabled = false)
	public void writePerformance() {
		String dbName = "writepoints_" + System.currentTimeMillis();
      influxDB.createDatabase(dbName);

		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < COUNT; i++) {

			BatchPoints batchPoints = BatchPoints
					.database(dbName)
					.tag("blubber", "bla")
					.retentionPolicy("default")
					.build();
			for (int j = 0; j < POINT_COUNT; j++) {
				Point point = Point
						.measurement("cpu")
						.field("idle", j)
						.field("user", 2 * j)
						.field("system", 3 * j)
						.build();
				batchPoints.point(point);
			}

         influxDB.write(batchPoints);
		}
      LOGGER.info("WritePoints for {} writes of {}. Points took: {}", COUNT, POINT_COUNT, watch);
      influxDB.deleteDatabase(dbName);
	}

	@Test
	public void maxWritePointsPerformance() {
		String dbName = "d";
      influxDB.createDatabase(dbName);
      influxDB.enableBatch(100000, 60, TimeUnit.SECONDS);

		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < 2000000; i++) {
			Point point = Point.measurement("s").field("v", 1).build();
         influxDB.write(dbName, "default", point);
		}
      LOGGER.info("5Mio points: {}", watch);
      influxDB.deleteDatabase(dbName);
	}
}

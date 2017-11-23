package org.influxdb;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(JUnitPlatform.class)
public class PerformanceTests {
	private InfluxDB influxDB;
	private final static int COUNT = 1;
	private final static int POINT_COUNT = 100000;
	private final static int SINGLE_POINT_COUNT = 10000;
	
	private final static int UDP_PORT = 8089;
	private final static String UDP_DATABASE = "udp";

	@BeforeEach
	public void setUp() {
		this.influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true), "root", "root");
		this.influxDB.setLogLevel(LogLevel.NONE);
		this.influxDB.createDatabase(UDP_DATABASE);
	}
	
	/**
	 * delete UDP database after all tests end.
	 */
	@AfterEach
	public void cleanup(){
		this.influxDB.deleteDatabase(UDP_DATABASE);
	}

	@Test
	public void testWriteSinglePointPerformance() {
		String dbName = "write_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		this.influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
		long start = System.currentTimeMillis();
		for (int j = 0; j < SINGLE_POINT_COUNT; j++) {
			Point point = Point.measurement("cpu")
					.addField("idle", (double) j)
					.addField("user", 2.0 * j)
					.addField("system", 3.0 * j).build();
			this.influxDB.write(dbName, rp, point);
		}
		this.influxDB.disableBatch();
		System.out.println("Single Point Write for " + SINGLE_POINT_COUNT + " writes of Points took:" + (System.currentTimeMillis() - start));
		this.influxDB.deleteDatabase(dbName);
	}

	@Disabled
	@Test
	public void testWritePerformance() {
		String dbName = "writepoints_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

		long start = System.currentTimeMillis();
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
		System.out.println("WritePoints for " + COUNT + " writes of " + POINT_COUNT + " Points took:" + (System.currentTimeMillis() - start));
		this.influxDB.deleteDatabase(dbName);
	}

	@Test
	public void testMaxWritePointsPerformance() {
		String dbName = "d";
		this.influxDB.createDatabase(dbName);
		this.influxDB.enableBatch(100000, 60, TimeUnit.SECONDS);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

		long start = System.currentTimeMillis();
		for (int i = 0; i < 2000000; i++) {
			Point point = Point.measurement("s").addField("v", 1.0).build();
			this.influxDB.write(dbName, rp, point);
		}
		System.out.println("5Mio points:" + (System.currentTimeMillis() - start));
		this.influxDB.deleteDatabase(dbName);
	}

	@Test
	public void testWriteCompareUDPPerformanceForBatchWithSinglePoints() {
		//prepare data
		List<String> lineProtocols = new ArrayList<String>();
		for (int i = 0; i < 1000; i++) {
		    Point point = Point.measurement("udp_single_poit").addField("v", i).build();
		    lineProtocols.add(point.lineProtocol());
		}

		String dbName = "write_compare_udp_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		this.influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);

		//write batch of 1000 single string.
		long start = System.currentTimeMillis();
		this.influxDB.write(UDP_PORT, lineProtocols);
		long elapsedForBatchWrite = System.currentTimeMillis() - start;
		System.out.println("performance(ms):write udp with batch of 1000 string:" + elapsedForBatchWrite);

		//write 1000 single string by udp.
		start = System.currentTimeMillis();
		for (String lineProtocol: lineProtocols){
		    this.influxDB.write(UDP_PORT, lineProtocol);
		}
		this.influxDB.deleteDatabase(dbName);

		long elapsedForSingleWrite = System.currentTimeMillis() - start;
		System.out.println("performance(ms):write udp with 1000 single strings:" + elapsedForSingleWrite);

		Assertions.assertTrue(elapsedForSingleWrite - elapsedForBatchWrite > 0);
	}

}

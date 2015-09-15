package org.influxdb;

import org.influxdb.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Test the InfluxDB API.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
@Test
public class InfluxDBTest extends AbstractTest {
   private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBTest.class);

	/**
	 * Test for a ping.
	 */
	public void testPing() {
		Pong result = influxDB.ping();
		Assert.assertNotNull(result);
		Assert.assertNotEquals(result.getVersion(), "unknown");
	}

	/**
	 * Test that version works.
	 */
	public void testVersion() {
		String version = influxDB.version();
		Assert.assertNotNull(version);
		Assert.assertFalse(version.contains("unknown"));
	}

	/**
	 * Simple Test for a query.
	 */
	public void testQuery() {
      influxDB.query(new Query("CREATE DATABASE mydb2", "mydb"));
      influxDB.query(new Query("DROP DATABASE mydb2", "mydb"));
	}

	/**
	 * Test that describe Databases works.
	 */
	public void testDescribeDatabases() {
		String dbName = "unittest_" + System.currentTimeMillis();
      influxDB.createDatabase(dbName);
      influxDB.describeDatabases();
		List<String> result = influxDB.describeDatabases();
		Assert.assertNotNull(result);
		Assert.assertTrue(result.size() > 0);
		boolean found = false;
		for (String database : result) {
			if (database.equals(dbName)) {
				found = true;
				break;
			}

		}
		Assert.assertTrue(found, "It is expected that describeDataBases contents the newly create database.");
	}

	/**
	 * Test that writing to the new lineprotocol.
	 */
	public void testWrite() {
		String dbName = "write_unittest_" + System.currentTimeMillis();
      influxDB.createDatabase(dbName);

		BatchPoints batchPoints = BatchPoints.database(dbName).tag("async", "true").retentionPolicy("default").build();
		Point point1 = Point
				.measurement("cpu")
				.tag("atag", "test")
				.field("idle", 90L)
				.field("usertime", 9L)
				.field("system", 1L)
				.build();
		Point point2 = Point.measurement("disk").tag("atag", "test").field("used", 80L).field("free", 1L).build();
		batchPoints.point(point1);
		batchPoints.point(point2);
      influxDB.write(batchPoints);
		Query query = new Query("SELECT * FROM cpu GROUP BY atag,async", dbName);
		QueryResult result = influxDB.query(query);
		Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
      influxDB.deleteDatabase(dbName);
	}
}

package org.influxdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test the InfluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
@Test
public class InfluxDBTest {

	private InfluxDB influxDB;

	/**
	 * Create a influxDB connection before all tests start.
	 *
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@BeforeClass
	public void setUp() throws InterruptedException, IOException {
		this.influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":8086", "root", "root");
		boolean influxDBstarted = false;
		do {
			Pong response;
			try {
				response = this.influxDB.ping();
				System.out.println(response);
				if (!response.getVersion().equalsIgnoreCase("unknown")) {
					influxDBstarted = true;
				}
			} catch (Exception e) {
				// NOOP intentional
				e.printStackTrace();
			}
			Thread.sleep(100L);
		} while (!influxDBstarted);
		this.influxDB.setLogLevel(LogLevel.FULL);
		// String logs = CharStreams.toString(new InputStreamReader(containerLogsStream,
		// Charsets.UTF_8));
		System.out.println("##################################################################################");
		// System.out.println("Container Logs: \n" + logs);
		System.out.println("#  Connected to InfluxDB Version: " + this.influxDB.version() + " #");
		System.out.println("##################################################################################");
	}

	/**
	 * Test for a ping.
	 */
	@Test(enabled = true)
	public void testPing() {
		Pong result = this.influxDB.ping();
		Assert.assertNotNull(result);
		Assert.assertNotEquals(result.getVersion(), "unknown");
	}

	/**
	 * Test that version works.
	 */
	@Test(enabled = true)
	public void testVersion() {
		String version = this.influxDB.version();
		Assert.assertNotNull(version);
		Assert.assertFalse(version.contains("unknown"));
	}

	/**
	 * Simple Test for a query.
	 */
	@Test(enabled = true)
	public void testQuery() {
		this.influxDB.query(new Query("CREATE DATABASE mydb2", "mydb"));
		this.influxDB.query(new Query("DROP DATABASE mydb2", "mydb"));
	}

	/**
	 * Test that describe Databases works.
	 */
	@Test(enabled = true)
	public void testDescribeDatabases() {
		String dbName = "unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		this.influxDB.describeDatabases();
		List<String> result = this.influxDB.describeDatabases();
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
	@Test(enabled = true)
	public void testWrite() {
		String dbName = "write_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		BatchPoints batchPoints = BatchPoints.database(dbName).tag("async", "true").retentionPolicy("default").build();
		Point point1 = Point
				.measurement("cpu")
				.tag("atag", "test")
				.addField("idle", 90L)
				.addField("usertime", 9L)
				.addField("system", 1L)
				.build();
		Point point2 = Point.measurement("disk").tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
		batchPoints.point(point1);
		batchPoints.point(point2);
		this.influxDB.write(batchPoints);
		Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
		QueryResult result = this.influxDB.query(query);
		Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
		this.influxDB.deleteDatabase(dbName);
	}

    /**
     * Test writing to the database using string protocol.
     */
    @Test(enabled = true)
    public void testWriteStringData() {
        String dbName = "write_unittest_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);

        this.influxDB.write(dbName, "default", InfluxDB.ConsistencyLevel.ONE, "cpu,atag=test idle=90,usertime=9,system=1");
        Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
        QueryResult result = this.influxDB.query(query);
        Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
        this.influxDB.deleteDatabase(dbName);
    }

    /**
     * Test writing multiple records to the database using string protocol.
     */
    @Test(enabled = true)
    public void testWriteMultipleStringData() {
        String dbName = "write_unittest_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);

        this.influxDB.write(dbName, "default", InfluxDB.ConsistencyLevel.ONE, "cpu,atag=test1 idle=100,usertime=10,system=1\ncpu,atag=test2 idle=200,usertime=20,system=2\ncpu,atag=test3 idle=300,usertime=30,system=3");
        Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
        QueryResult result = this.influxDB.query(query);

        Assert.assertEquals(result.getResults().get(0).getSeries().size(), 3);
        Assert.assertEquals(result.getResults().get(0).getSeries().get(0).getTags().get("atag"), "test1");
        Assert.assertEquals(result.getResults().get(0).getSeries().get(1).getTags().get("atag"), "test2");
        Assert.assertEquals(result.getResults().get(0).getSeries().get(2).getTags().get("atag"), "test3");
        this.influxDB.deleteDatabase(dbName);
    }

    /**
     * Test writing multiple separate records to the database using string protocol.
     */
    @Test(enabled = true)
    public void testWriteMultipleStringDataLines() {
        String dbName = "write_unittest_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);

        this.influxDB.write(dbName, "default", InfluxDB.ConsistencyLevel.ONE, Arrays.asList(
                "cpu,atag=test1 idle=100,usertime=10,system=1",
                "cpu,atag=test2 idle=200,usertime=20,system=2",
                "cpu,atag=test3 idle=300,usertime=30,system=3"
        ));
        Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
        QueryResult result = this.influxDB.query(query);

        Assert.assertEquals(result.getResults().get(0).getSeries().size(), 3);
        Assert.assertEquals(result.getResults().get(0).getSeries().get(0).getTags().get("atag"), "test1");
        Assert.assertEquals(result.getResults().get(0).getSeries().get(1).getTags().get("atag"), "test2");
        Assert.assertEquals(result.getResults().get(0).getSeries().get(2).getTags().get("atag"), "test3");
        this.influxDB.deleteDatabase(dbName);
    }

	/**
	 * Test that creating database which name is composed of numbers only works
	 */
	@Test(enabled = true)
	public void testCreateNumericNamedDatabase() {
		String numericDbName = "123";

		this.influxDB.createDatabase(numericDbName);
		List<String> result = this.influxDB.describeDatabases();
		Assert.assertTrue(result.contains(numericDbName));
		this.influxDB.deleteDatabase(numericDbName);
	}

	/**
	 * Test the implementation of {@link InfluxDB#isBatchEnabled()}.
	 */
	@Test(enabled = true)
	public void testIsBatchEnabled() {
		Assert.assertFalse(this.influxDB.isBatchEnabled());

		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
		Assert.assertTrue(this.influxDB.isBatchEnabled());

		this.influxDB.disableBatch();
		Assert.assertFalse(this.influxDB.isBatchEnabled());
	}
}

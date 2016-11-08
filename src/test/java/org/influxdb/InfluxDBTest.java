package org.influxdb;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.*;
import org.influxdb.impl.InfluxDBImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Test the InfluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class InfluxDBTest {

	private InfluxDB influxDB;
	private final static int UDP_PORT = 8089;
	private final static String UDP_DATABASE = "udp";

	/**
	 * Create a influxDB connection before all tests start.
	 *
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Before
	public void setUp() throws InterruptedException, IOException {
		this.influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true), "admin", "admin");
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
		this.influxDB.createDatabase(UDP_DATABASE);
		// String logs = CharStreams.toString(new InputStreamReader(containerLogsStream,
		// Charsets.UTF_8));
		System.out.println("##################################################################################");
		// System.out.println("Container Logs: \n" + logs);
		System.out.println("#  Connected to InfluxDB Version: " + this.influxDB.version() + " #");
		System.out.println("##################################################################################");
	}
	
	/**
	 * delete UDP database after all tests end.
	 */
	//@After
	public void clearup(){
		this.influxDB.deleteDatabase(UDP_DATABASE);
	}

	/**
	 * Test for a ping.
	 */
	@Test
	public void testPing() {
		Pong result = this.influxDB.ping();
		Assert.assertNotNull(result);
		Assert.assertNotEquals(result.getVersion(), "unknown");
	}

	/**
	 * Test that version works.
	 */
	@Test
	public void testVersion() {
		String version = this.influxDB.version();
		Assert.assertNotNull(version);
		Assert.assertFalse(version.contains("unknown"));
	}

	/**
	 * Simple Test for a query.
	 */
	@Test
	public void testQuery() {
		this.influxDB.query(new Query("CREATE DATABASE mydb2", "mydb"));
		this.influxDB.query(new Query("DROP DATABASE mydb2", "mydb"));
	}

	/**
	 * Test that describe Databases works.
	 */
	@Test
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
		Assert.assertTrue("It is expected that describeDataBases contents the newly create database.", found);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that writing to the new lineprotocol.
	 */
	@Test
	public void testWrite() {
		String dbName = "write_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
		BatchPoints batchPoints = BatchPoints.database(dbName).tag("async", "true").retentionPolicy(rp).build();
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
    @Test
    public void testWriteStringData() {
        String dbName = "write_unittest_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);
        String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
        this.influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, "cpu,atag=test idle=90,usertime=9,system=1");
        Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
        QueryResult result = this.influxDB.query(query);
        Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
        this.influxDB.deleteDatabase(dbName);
    }

    /**
     * Test writing to the database using string protocol through UDP.
     */
    @Test
    public void testWriteStringDataThroughUDP() {
        String measurement = TestUtils.getRandomMeasurement();
        this.influxDB.write(UDP_PORT, measurement + ",atag=test idle=90,usertime=9,system=1");
        //write with UDP may be executed on server after query with HTTP. so sleep 1s to handle this case
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
        QueryResult result = this.influxDB.query(query);
        Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
    }

    /**
     * Test writing multiple records to the database using string protocol through UDP.
     */
    @Test
    public void testWriteMultipleStringDataThroughUDP() {
        String measurement = TestUtils.getRandomMeasurement();
        this.influxDB.write(UDP_PORT, measurement + ",atag=test1 idle=100,usertime=10,system=1\n" + 
                                      measurement + ",atag=test2 idle=200,usertime=20,system=2\n" + 
                                      measurement + ",atag=test3 idle=300,usertime=30,system=3");
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
        QueryResult result = this.influxDB.query(query);

        Assert.assertEquals(3, result.getResults().get(0).getSeries().size());
        Assert.assertEquals(result.getResults().get(0).getSeries().get(0).getTags().get("atag"), "test1");
        Assert.assertEquals(result.getResults().get(0).getSeries().get(1).getTags().get("atag"), "test2");
        Assert.assertEquals(result.getResults().get(0).getSeries().get(2).getTags().get("atag"), "test3");
    }

    /**
     * Test writing multiple separate records to the database using string protocol through UDP.
     */
    @Test
    public void testWriteMultipleStringDataLinesThroughUDP() {
        String measurement = TestUtils.getRandomMeasurement();
        this.influxDB.write(UDP_PORT, Arrays.asList(
                measurement + ",atag=test1 idle=100,usertime=10,system=1",
                measurement + ",atag=test2 idle=200,usertime=20,system=2",
                measurement + ",atag=test3 idle=300,usertime=30,system=3"
        ));
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
        QueryResult result = this.influxDB.query(query);

        Assert.assertEquals(3, result.getResults().get(0).getSeries().size());
        Assert.assertEquals(result.getResults().get(0).getSeries().get(0).getTags().get("atag"), "test1");
        Assert.assertEquals(result.getResults().get(0).getSeries().get(1).getTags().get("atag"), "test2");
        Assert.assertEquals(result.getResults().get(0).getSeries().get(2).getTags().get("atag"), "test3"); 
    }

    /**
     * Test writing multiple records to the database using string protocol.
     */
    @Test
    public void testWriteMultipleStringData() {
        String dbName = "write_unittest_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);
        String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

        this.influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, "cpu,atag=test1 idle=100,usertime=10,system=1\ncpu,atag=test2 idle=200,usertime=20,system=2\ncpu,atag=test3 idle=300,usertime=30,system=3");
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
    @Test
    public void testWriteMultipleStringDataLines() {
        String dbName = "write_unittest_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);
        String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

        this.influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, Arrays.asList(
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
	@Test
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
	@Test
	public void testIsBatchEnabled() {
		Assert.assertFalse(this.influxDB.isBatchEnabled());

		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
		Assert.assertTrue(this.influxDB.isBatchEnabled());

		this.influxDB.disableBatch();
		Assert.assertFalse(this.influxDB.isBatchEnabled());
	}
	
	/**
	 * Test the implementation of {@link InfluxDB#enableBatch(int, int, TimeUnit, ThreadFactory)}.
	 */
	@Test
	public void testBatchEnabledWithThreadFactory() {
		final String threadName = "async_influxdb_write";
		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS, new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				Thread thread = new Thread(r);
				thread.setName(threadName);
				return thread;
			}
		});
		Set<Thread> threads = Thread.getAllStackTraces().keySet();
		boolean existThreadWithSettedName = false;
		for(Thread thread: threads){
			if(thread.getName().equalsIgnoreCase(threadName)){
				existThreadWithSettedName = true;
				break;
			}
			
		}
		Assert.assertTrue(existThreadWithSettedName);
		this.influxDB.disableBatch();
	}

	@Test(expected = NullPointerException.class)
	public void testBatchEnabledWithThreadFactoryIsNull() {
		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS, null);
	}
	
	/**
	 * Test the implementation of {@link InfluxDBImpl#InfluxDBImpl(String, String, String, okhttp3.OkHttpClient.Builder)}.
	 */
	@Test(expected = RuntimeException.class)
	public void testWrongHostForInfluxdb(){
		String errorHost = "10.224.2.122_error_host";
		InfluxDBFactory.connect("http://" + errorHost + ":" + TestUtils.getInfluxPORT(true));
	}

	/**
	 * Test the implementation of {@link InfluxDB#close()}.
	 */
	@Test
	public void testCloseInfluxDBClient() {
		InfluxDB influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true), "admin", "admin");
		influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
		Assert.assertTrue(influxDB.isBatchEnabled());
		influxDB.close();
		Assert.assertFalse(influxDB.isBatchEnabled());
	}

}

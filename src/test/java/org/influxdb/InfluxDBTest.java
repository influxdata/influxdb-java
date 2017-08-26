package org.influxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    @Rule public final ExpectedException exception = ExpectedException.none();
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
				if (!response.getVersion().equalsIgnoreCase("unknown")) {
					influxDBstarted = true;
				}
			} catch (Exception e) {
				// NOOP intentional
				e.printStackTrace();
			}
			Thread.sleep(100L);
		} while (!influxDBstarted);
		this.influxDB.setLogLevel(LogLevel.NONE);
		this.influxDB.createDatabase(UDP_DATABASE);
        System.out.println("################################################################################## ");
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
	 * Tests for callback query.
	 */
	@Test
	public void testCallbackQuery() throws Throwable {
		final AsyncResult<QueryResult> result = new AsyncResult<>();
		final Consumer<QueryResult> firstQueryConsumer  = new Consumer<QueryResult>() {
			@Override
			public void accept(QueryResult queryResult) {
				influxDB.query(new Query("DROP DATABASE mydb2", "mydb"), result.resultConsumer, result.errorConsumer);
			}
		};

		this.influxDB.query(new Query("CREATE DATABASE mydb2", "mydb"), firstQueryConsumer, result.errorConsumer);

		// Will throw exception in case of error.
		result.result();
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
	 * Test that Database exists works.
	 */
	@Test
	public void testDatabaseExists() {
		String existentdbName = "unittest_1";
		String notExistentdbName = "unittest_2";
		this.influxDB.createDatabase(existentdbName);
		boolean checkDbExistence = this.influxDB.databaseExists(existentdbName);
		Assert.assertTrue("It is expected that databaseExists return true for " + existentdbName + " database", checkDbExistence);
		checkDbExistence = this.influxDB.databaseExists(notExistentdbName);
		Assert.assertFalse("It is expected that databaseExists return false for " + notExistentdbName + " database", checkDbExistence);
		this.influxDB.deleteDatabase(existentdbName);
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
	 *  Test the implementation of {@link InfluxDB#write(int, Point)}'s sync support.
	 */
	@Test
	public void testSyncWritePointThroughUDP() {
		this.influxDB.disableBatch();
		String measurement = TestUtils.getRandomMeasurement();
		Point point = Point.measurement(measurement).tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
		this.influxDB.write(UDP_PORT, point);
		Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
		Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
		QueryResult result = this.influxDB.query(query);
		Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
	}
	
	/**
	 *  Test the implementation of {@link InfluxDB#write(int, Point)}'s async support.
	 */
	@Test
	public void testAsyncWritePointThroughUDP() {
		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
		try{
			Assert.assertTrue(this.influxDB.isBatchEnabled());
			String measurement = TestUtils.getRandomMeasurement();
			Point point = Point.measurement(measurement).tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
			this.influxDB.write(UDP_PORT, point);
			Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
			Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
			QueryResult result = this.influxDB.query(query);
			Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
		}finally{
			this.influxDB.disableBatch();
		}
	}
	
    
    /**
     *  Test the implementation of {@link InfluxDB#write(int, Point)}'s async support.
     */
    @Test(expected = RuntimeException.class)
    public void testAsyncWritePointThroughUDPFail() {
        this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
        try{
            Assert.assertTrue(this.influxDB.isBatchEnabled());
            String measurement = TestUtils.getRandomMeasurement();
            Point point = Point.measurement(measurement).tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
            Thread.currentThread().interrupt();
            this.influxDB.write(UDP_PORT, point);
        }finally{
            this.influxDB.disableBatch();
        }
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
	 * Test writing to the database using string protocol with simpler interface.
	 */
	@Test
	public void testWriteStringDataSimple() {
		String dbName = "write_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
		this.influxDB.setDatabase(dbName);
		this.influxDB.setRetentionPolicy(rp);
		this.influxDB.write("cpu,atag=test idle=90,usertime=9,system=1");
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
        //write with UDP may be executed on server after query with HTTP. so sleep 2s to handle this case
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
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
     * When batch of points' size is over UDP limit, the expected exception
     * is java.lang.RuntimeException: java.net.SocketException:
     * The message is larger than the maximum supported by the underlying transport: Datagram send failed
     * @throws Exception
     */
    @Test(expected = RuntimeException.class)
    public void writeMultipleStringDataLinesOverUDPLimit() throws Exception {
        //prepare data
        List<String> lineProtocols = new ArrayList<String>();
        int i = 0;
        int length = 0;
        while ( true ) {
            Point point = Point.measurement("udp_single_poit").addField("v", i).build();
            String lineProtocol = point.lineProtocol();
            length += (lineProtocol.getBytes("utf-8")).length;
            lineProtocols.add(lineProtocol);
            if( length > 65535 ){
                break;
            }
        }
        //write batch of string which size is over 64K
        this.influxDB.write(UDP_PORT, lineProtocols);
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
	 * Test writing multiple records to the database using string protocol with simpler interface.
	 */
	@Test
	public void testWriteMultipleStringDataSimple() {
		String dbName = "write_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
		this.influxDB.setDatabase(dbName);
		this.influxDB.setRetentionPolicy(rp);

		this.influxDB.write("cpu,atag=test1 idle=100,usertime=10,system=1\ncpu,atag=test2 idle=200,usertime=20,system=2\ncpu,atag=test3 idle=300,usertime=30,system=3");
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
	 * Test writing multiple separate records to the database using string protocol with simpler interface.
	 */
	@Test
	public void testWriteMultipleStringDataLinesSimple() {
		String dbName = "write_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
		this.influxDB.setDatabase(dbName);
		this.influxDB.setRetentionPolicy(rp);

		this.influxDB.write(Arrays.asList(
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
     * Test that creating database which name is empty will throw expected exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateEmptyNamedDatabase() {
        String emptyName = "";
        this.influxDB.createDatabase(emptyName);
    }

    /**
     * Test that creating database which name contains -
     */
    @Test()
    public void testCreateDatabaseWithNameContainHyphen() {
        String databaseName = "123-456";
        this.influxDB.createDatabase(databaseName);
        try {
            List<String> result = this.influxDB.describeDatabases();
            Assert.assertTrue(result.contains(databaseName));
        } finally {
            this.influxDB.deleteDatabase(databaseName);
        }
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

	@Test(expected = IllegalStateException.class)
	public void testBatchEnabledTwice() {
		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
		try{
			this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
		} finally {
			this.influxDB.disableBatch();
		}
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

    /**
     * Test writing multiple separate records to the database by Gzip compress
     */
    @Test
    public void testWriteEnableGzip() {
        InfluxDB influxDBForTestGzip = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true), "admin", "admin");
        String dbName = "write_unittest_" + System.currentTimeMillis();
        try {
            influxDBForTestGzip.setLogLevel(LogLevel.NONE);
            influxDBForTestGzip.enableGzip();
            influxDBForTestGzip.createDatabase(dbName);
            String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

            influxDBForTestGzip.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, Arrays.asList(
                    "cpu,atag=test1 idle=100,usertime=10,system=1",
                    "cpu,atag=test2 idle=200,usertime=20,system=2",
                    "cpu,atag=test3 idle=300,usertime=30,system=3"
            ));
            Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
            QueryResult result = influxDBForTestGzip.query(query);

            Assert.assertEquals(result.getResults().get(0).getSeries().size(), 3);
            Assert.assertEquals(result.getResults().get(0).getSeries().get(0).getTags().get("atag"), "test1");
            Assert.assertEquals(result.getResults().get(0).getSeries().get(1).getTags().get("atag"), "test2");
            Assert.assertEquals(result.getResults().get(0).getSeries().get(2).getTags().get("atag"), "test3");
        } finally {
            influxDBForTestGzip.deleteDatabase(dbName);
            influxDBForTestGzip.close();
        }
    }

    /**
     * Test the implementation of flag control for gzip such as:
     * {@link InfluxDB#disableGzip()}} and {@link InfluxDB#isBatchEnabled()}},etc
     */
    @Test
    public void testWriteEnableGzipAndDisableGzip() {
        InfluxDB influxDBForTestGzip = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true), "admin", "admin");
        try {
            //test default: gzip is disable
            Assert.assertFalse(influxDBForTestGzip.isGzipEnabled());
            influxDBForTestGzip.enableGzip();
            Assert.assertTrue(influxDBForTestGzip.isGzipEnabled());
            influxDBForTestGzip.disableGzip();
            Assert.assertFalse(influxDBForTestGzip.isGzipEnabled());
        } finally {
            influxDBForTestGzip.close();
        }
    }

    /**
     * Test chunking.
     * @throws InterruptedException
     */
    @Test
    public void testChunking() throws InterruptedException {
        if (this.influxDB.version().startsWith("0.") || this.influxDB.version().startsWith("1.0")) {
            // do not test version 0.13 and 1.0
            return;
        }
        String dbName = "write_unittest_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);
        String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());
        BatchPoints batchPoints = BatchPoints.database(dbName).retentionPolicy(rp).build();
        Point point1 = Point.measurement("disk").tag("atag", "a").addField("used", 60L).addField("free", 1L).build();
        Point point2 = Point.measurement("disk").tag("atag", "b").addField("used", 70L).addField("free", 2L).build();
        Point point3 = Point.measurement("disk").tag("atag", "c").addField("used", 80L).addField("free", 3L).build();
        batchPoints.point(point1);
        batchPoints.point(point2);
        batchPoints.point(point3);
        this.influxDB.write(batchPoints);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>();
        Query query = new Query("SELECT * FROM disk", dbName);
        this.influxDB.query(query, 2, new Consumer<QueryResult>() {
            @Override
            public void accept(QueryResult result) {
                queue.add(result);
            }});

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        this.influxDB.deleteDatabase(dbName);

        QueryResult result = queue.poll(20, TimeUnit.SECONDS);
        Assert.assertNotNull(result);
        System.out.println(result);
        Assert.assertEquals(2, result.getResults().get(0).getSeries().get(0).getValues().size());

        result = queue.poll(20, TimeUnit.SECONDS);
        Assert.assertNotNull(result);
        System.out.println(result);
        Assert.assertEquals(1, result.getResults().get(0).getSeries().get(0).getValues().size());

        result = queue.poll(20, TimeUnit.SECONDS);
        Assert.assertNotNull(result);
        System.out.println(result);
        Assert.assertEquals("DONE", result.getError());
    }

    /**
     * Test chunking edge case.
     * @throws InterruptedException
     */
    @Test
    public void testChunkingFail() throws InterruptedException {
        if (this.influxDB.version().startsWith("0.") || this.influxDB.version().startsWith("1.0")) {
            // do not test version 0.13 and 1.0
            return;
        }
        String dbName = "write_unittest_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Query query = new Query("UNKNOWN_QUERY", dbName);
        this.influxDB.query(query, 10, new Consumer<QueryResult>() {
            @Override
            public void accept(QueryResult result) {
                countDownLatch.countDown();
            }
        });
        this.influxDB.deleteDatabase(dbName);
        Assert.assertFalse(countDownLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test chunking on 0.13 and 1.0.
     * @throws InterruptedException
     */
    @Test()
    public void testChunkingOldVersion() throws InterruptedException {

        if (this.influxDB.version().startsWith("0.") || this.influxDB.version().startsWith("1.0")) {

            this.exception.expect(RuntimeException.class);
            String dbName = "write_unittest_" + System.currentTimeMillis();
            Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
            this.influxDB.query(query, 10, new Consumer<QueryResult>() {
                @Override
                public void accept(QueryResult result) {
                }
            });
        }
    }

    @Test
    public void testFlushPendingWritesWhenBatchingEnabled() {
        String dbName = "flush_tests_" + System.currentTimeMillis();
        try {
            this.influxDB.createDatabase(dbName);

            // Enable batching with a very large buffer and flush interval so writes will be triggered by our call to flush().
            this.influxDB.enableBatch(Integer.MAX_VALUE, Integer.MAX_VALUE, TimeUnit.HOURS);

            String measurement = TestUtils.getRandomMeasurement();
            Point point = Point.measurement(measurement).tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
            this.influxDB.write(dbName, TestUtils.defaultRetentionPolicy(this.influxDB.version()), point);
            this.influxDB.flush();

            Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", dbName);
            QueryResult result = this.influxDB.query(query);
            Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
        } finally {
            this.influxDB.deleteDatabase(dbName);
            this.influxDB.disableBatch();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testFlushThrowsIfBatchingIsNotEnabled() {
        Assert.assertFalse(this.influxDB.isBatchEnabled());
        this.influxDB.flush();
    }

	/**
	 * Test creation and deletion of retention policies
	 */
	@Test
	public void testCreateDropRetentionPolicies() {
		String dbName = "rpTest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		this.influxDB.createRetentionPolicy("testRP1", dbName, "30h", 2, false);
		this.influxDB.createRetentionPolicy("testRP2", dbName, "10d", "20m", 2, false);
		this.influxDB.createRetentionPolicy("testRP3", dbName, "2d4w", "20m", 2);

		Query query = new Query("SHOW RETENTION POLICIES", dbName);
		QueryResult result = this.influxDB.query(query);
		Assert.assertNull(result.getError());
		List<List<Object>> retentionPolicies = result.getResults().get(0).getSeries().get(0).getValues();
		Assert.assertTrue(retentionPolicies.get(1).contains("testRP1"));
		Assert.assertTrue(retentionPolicies.get(2).contains("testRP2"));
		Assert.assertTrue(retentionPolicies.get(3).contains("testRP3"));

		this.influxDB.dropRetentionPolicy("testRP1", dbName);
		this.influxDB.dropRetentionPolicy("testRP2", dbName);
		this.influxDB.dropRetentionPolicy("testRP3", dbName);

		result = this.influxDB.query(query);
		Assert.assertNull(result.getError());
		retentionPolicies = result.getResults().get(0).getSeries().get(0).getValues();
		Assert.assertTrue(retentionPolicies.size() == 1);
	}

}

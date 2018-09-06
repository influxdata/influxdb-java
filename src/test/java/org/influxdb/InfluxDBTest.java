package org.influxdb;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.BoundParameterQuery.QueryBuilder;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.influxdb.impl.InfluxDBImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import okhttp3.OkHttpClient;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Test the InfluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
@RunWith(JUnitPlatform.class)
public class InfluxDBTest {

	InfluxDB influxDB;
	private final static int UDP_PORT = 8089;
	final static String UDP_DATABASE = "udp";

	/**
	 * Create a influxDB connection before all tests start.
	 *
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@BeforeEach
	public void setUp() throws InterruptedException, IOException {
		this.influxDB = TestUtils.connectToInfluxDB();
		this.influxDB.createDatabase(UDP_DATABASE);
	}

	/**
	 * delete UDP database after all tests end.
	 */
	@AfterEach
	public void cleanup(){
		this.influxDB.deleteDatabase(UDP_DATABASE);
	}

	/**
	 * Test for a ping.
	 */
	@Test
	public void testPing() {
		Pong result = this.influxDB.ping();
		Assertions.assertNotNull(result);
		Assertions.assertNotEquals(result.getVersion(), "unknown");
	}

	/**
	 * Test that version works.
	 */
	@Test
	public void testVersion() {
		String version = this.influxDB.version();
		Assertions.assertNotNull(version);
		Assertions.assertFalse(version.contains("unknown"));
	}

	/**
	 * Simple Test for a query.
	 */
	@Test
	public void testQuery() {
		this.influxDB.query(new Query("CREATE DATABASE mydb2", "mydb"));
		this.influxDB.query(new Query("DROP DATABASE mydb2", "mydb"));
	}

  @Test
  public void testBoundParameterQuery() throws InterruptedException {
    // set up
    Point point = Point
        .measurement("cpu")
        .tag("atag", "test")
        .addField("idle", 90L)
        .addField("usertime", 9L)
        .addField("system", 1L)
        .build();
    this.influxDB.setDatabase(UDP_DATABASE);
    this.influxDB.write(point);

    // test
    Query query = QueryBuilder.newQuery("SELECT * FROM cpu WHERE atag = $atag")
        .forDatabase(UDP_DATABASE)
        .bind("atag", "test")
        .create();
    QueryResult result = this.influxDB.query(query);
    Assertions.assertTrue(result.getResults().get(0).getSeries().size() == 1);
    Series series = result.getResults().get(0).getSeries().get(0);
    Assertions.assertTrue(series.getValues().size() == 1);

    result = this.influxDB.query(query, TimeUnit.SECONDS);
    Assertions.assertTrue(result.getResults().get(0).getSeries().size() == 1);
    series = result.getResults().get(0).getSeries().get(0);
    Assertions.assertTrue(series.getValues().size() == 1);

    Object waitForTestresults = new Object();
    Consumer<QueryResult> check = (queryResult) -> {
      Assertions.assertTrue(queryResult.getResults().get(0).getSeries().size() == 1);
      Series s = queryResult.getResults().get(0).getSeries().get(0);
      Assertions.assertTrue(s.getValues().size() == 1);
      synchronized (waitForTestresults) {
        waitForTestresults.notifyAll();
      }
    };
    this.influxDB.query(query, 10, check);
    synchronized (waitForTestresults) {
      waitForTestresults.wait(2000);
    }
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
		Assertions.assertNotNull(result);
		Assertions.assertTrue(result.size() > 0);
		boolean found = false;
		for (String database : result) {
			if (database.equals(dbName)) {
				found = true;
				break;
			}

		}
		Assertions.assertTrue(found, "It is expected that describeDataBases contents the newly create database.");
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
		Assertions.assertTrue(checkDbExistence, "It is expected that databaseExists return true for " + existentdbName + " database");
		checkDbExistence = this.influxDB.databaseExists(notExistentdbName);
		Assertions.assertFalse(checkDbExistence, "It is expected that databaseExists return false for " + notExistentdbName + " database");
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
		Assertions.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
		this.influxDB.deleteDatabase(dbName);
	}

    /**
     *  Test the implementation of {@link InfluxDB#write(int, Point)}'s async support.
     */
    @Test
    public void testAsyncWritePointThroughUDPFail() {
        this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
        try{
            Assertions.assertTrue(this.influxDB.isBatchEnabled());
            String measurement = TestUtils.getRandomMeasurement();
            Point point = Point.measurement(measurement).tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
            Thread.currentThread().interrupt();
            Assertions.assertThrows(RuntimeException.class, () -> {
				this.influxDB.write(UDP_PORT, point);
			});
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
        Assertions.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
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
		Assertions.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
		this.influxDB.deleteDatabase(dbName);
	}

    /**
     * When batch of points' size is over UDP limit, the expected exception
     * is java.lang.RuntimeException: java.net.SocketException:
     * The message is larger than the maximum supported by the underlying transport: Datagram send failed
     * @throws Exception
     */
    @Test
    public void testWriteMultipleStringDataLinesOverUDPLimit() throws Exception {
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
        Assertions.assertThrows(RuntimeException.class, () -> {
			this.influxDB.write(UDP_PORT, lineProtocols);
		});
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

        Assertions.assertEquals(result.getResults().get(0).getSeries().size(), 3);
        Assertions.assertEquals("test1", result.getResults().get(0).getSeries().get(0).getTags().get("atag"));
        Assertions.assertEquals("test2", result.getResults().get(0).getSeries().get(1).getTags().get("atag"));
        Assertions.assertEquals("test3", result.getResults().get(0).getSeries().get(2).getTags().get("atag"));
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

		Assertions.assertEquals(result.getResults().get(0).getSeries().size(), 3);
		Assertions.assertEquals("test1", result.getResults().get(0).getSeries().get(0).getTags().get("atag"));
		Assertions.assertEquals("test2", result.getResults().get(0).getSeries().get(1).getTags().get("atag"));
		Assertions.assertEquals("test3", result.getResults().get(0).getSeries().get(2).getTags().get("atag"));
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

        Assertions.assertEquals(result.getResults().get(0).getSeries().size(), 3);
        Assertions.assertEquals("test1", result.getResults().get(0).getSeries().get(0).getTags().get("atag"));
        Assertions.assertEquals("test2", result.getResults().get(0).getSeries().get(1).getTags().get("atag"));
        Assertions.assertEquals("test3", result.getResults().get(0).getSeries().get(2).getTags().get("atag"));
        this.influxDB.deleteDatabase(dbName);
    }

	/**
	 * Tests writing points using the time precision feature
	 * @throws Exception
	 */
	@Test
	public void testWriteBatchWithPrecision() throws Exception {
		// GIVEN a database and a measurement
		String dbName = "precision_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

		String measurement = TestUtils.getRandomMeasurement();

		// GIVEN a batch of points using second precision
		DateTimeFormatter formatter = DateTimeFormatter
				.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
				.withZone(ZoneId.of("UTC"));
		int t1 = 1485273600;
		Point p1 = Point
				.measurement(measurement)
				.addField("foo", 1d)
				.tag("device", "one")
				.time(t1, TimeUnit.SECONDS).build(); // 2017-01-27T16:00:00
		String timeP1 = formatter.format(Instant.ofEpochSecond(t1));

		int t2 = 1485277200;
		Point p2 = Point
				.measurement(measurement)
				.addField("foo", 2d)
				.tag("device", "two")
				.time(t2, TimeUnit.SECONDS).build(); // 2017-01-27T17:00:00
		String timeP2 = formatter.format(Instant.ofEpochSecond(t2));

		int t3 = 1485280800;
		Point p3 = Point
				.measurement(measurement)
				.addField("foo", 3d)
				.tag("device", "three")
				.time(t3, TimeUnit.SECONDS).build(); // 2017-01-27T18:00:00
		String timeP3 = formatter.format(Instant.ofEpochSecond(t3));

		BatchPoints batchPoints = BatchPoints
				.database(dbName)
				.retentionPolicy(rp)
				.precision(TimeUnit.SECONDS)
				.points(p1, p2, p3)
				.build();

		// WHEN I write the batch
		this.influxDB.write(batchPoints);

		// THEN the measure points have a timestamp with second precision
		QueryResult queryResult = this.influxDB.query(new Query("SELECT * FROM " + measurement, dbName));
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().size(), 3);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(0), timeP1);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(1).get(0), timeP2);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(2).get(0), timeP3);

		this.influxDB.deleteDatabase(dbName);
	}

	@Test
	public void testWriteBatchWithoutPrecision() throws Exception {
		// GIVEN a database and a measurement
		String dbName = "precision_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

		String measurement = TestUtils.getRandomMeasurement();

		// GIVEN a batch of points that has no specific precision
		long t1 = 1485273600000000100L;
		Point p1 = Point
				.measurement(measurement)
				.addField("foo", 1d)
				.tag("device", "one")
				.time(t1, TimeUnit.NANOSECONDS).build(); // 2017-01-27T16:00:00.000000100Z
		Double timeP1 = Double.valueOf(t1);

		long t2 = 1485277200000000200L;
		Point p2 = Point
				.measurement(measurement)
				.addField("foo", 2d)
				.tag("device", "two")
				.time(t2, TimeUnit.NANOSECONDS).build(); // 2017-01-27T17:00:00.000000200Z
		Double timeP2 = Double.valueOf(t2);

		long t3 = 1485280800000000300L;
		Point p3 = Point
				.measurement(measurement)
				.addField("foo", 3d)
				.tag("device", "three")
				.time(t3, TimeUnit.NANOSECONDS).build(); // 2017-01-27T18:00:00.000000300Z
		Double timeP3 = Double.valueOf(t3);

		BatchPoints batchPoints = BatchPoints
				.database(dbName)
				.retentionPolicy(rp)
				.points(p1, p2, p3)
				.build();

		// WHEN I write the batch
		this.influxDB.write(batchPoints);

		// THEN the measure points have a timestamp with second precision
		QueryResult queryResult = this.influxDB.query(new Query("SELECT * FROM " + measurement, dbName), TimeUnit.NANOSECONDS);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().size(), 3);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(0), timeP1);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(1).get(0), timeP2);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(2).get(0), timeP3);

		this.influxDB.deleteDatabase(dbName);
	}

	@Test
	public void testWriteRecordsWithPrecision() throws Exception {
		// GIVEN a database and a measurement
		String dbName = "precision_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		String rp = TestUtils.defaultRetentionPolicy(this.influxDB.version());

		String measurement = TestUtils.getRandomMeasurement();

		// GIVEN a set of records using second precision
		DateTimeFormatter formatter = DateTimeFormatter
				.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
				.withZone(ZoneId.of("UTC"));
		List<String> records = new ArrayList<>();
		records.add(measurement + ",atag=test1 idle=100,usertime=10,system=1 1485273600");
		String timeP1 = formatter.format(Instant.ofEpochSecond(1485273600));

		records.add(measurement + ",atag=test2 idle=200,usertime=20,system=2 1485277200");
		String timeP2 = formatter.format(Instant.ofEpochSecond(1485277200));

		records.add(measurement + ",atag=test3 idle=300,usertime=30,system=3 1485280800");
		String timeP3 = formatter.format(Instant.ofEpochSecond(1485280800));

		// WHEN I write the batch
		this.influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, TimeUnit.SECONDS, records);

		// THEN the measure points have a timestamp with second precision
		QueryResult queryResult = this.influxDB.query(new Query("SELECT * FROM " + measurement, dbName));
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().size(), 3);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(0), timeP1);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(1).get(0), timeP2);
		Assertions.assertEquals(queryResult.getResults().get(0).getSeries().get(0).getValues().get(2).get(0), timeP3);
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

		Assertions.assertEquals(result.getResults().get(0).getSeries().size(), 3);
		Assertions.assertEquals("test1", result.getResults().get(0).getSeries().get(0).getTags().get("atag"));
		Assertions.assertEquals("test2", result.getResults().get(0).getSeries().get(1).getTags().get("atag"));
		Assertions.assertEquals("test3", result.getResults().get(0).getSeries().get(2).getTags().get("atag"));
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
		Assertions.assertTrue(result.contains(numericDbName));
		this.influxDB.deleteDatabase(numericDbName);
	}

    /**
     * Test that creating database which name is empty will throw expected exception
     */
    @Test
    public void testCreateEmptyNamedDatabase() {
        String emptyName = "";
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
			this.influxDB.createDatabase(emptyName);
		});
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
            Assertions.assertTrue(result.contains(databaseName));
        } finally {
            this.influxDB.deleteDatabase(databaseName);
        }
    }

	/**
	 * Test the implementation of {@link InfluxDB#isBatchEnabled()}.
	 */
	@Test
	public void testIsBatchEnabled() {
		Assertions.assertFalse(this.influxDB.isBatchEnabled());

		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
		Assertions.assertTrue(this.influxDB.isBatchEnabled());

		this.influxDB.disableBatch();
		Assertions.assertFalse(this.influxDB.isBatchEnabled());
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
		Assertions.assertTrue(existThreadWithSettedName);
		this.influxDB.disableBatch();
	}

	@Test
	public void testBatchEnabledWithThreadFactoryIsNull() {
		Assertions.assertThrows(NullPointerException.class, () -> {
			this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS, null);
		});
	}

	/**
	 * Test the implementation of {@link InfluxDBImpl#InfluxDBImpl(String, String, String, okhttp3.OkHttpClient.Builder)}.
	 */
	@Test
	public void testWrongHostForInfluxdb(){
		String errorHost = "10.224.2.122_error_host";
		Assertions.assertThrows(RuntimeException.class, () -> {
			InfluxDBFactory.connect("http://" + errorHost + ":" + TestUtils.getInfluxPORT(true));
		});
		
		String unresolvableHost = "a.b.c";
		Assertions.assertThrows(InfluxDBIOException.class, () -> {
		  InfluxDBFactory.connect("http://" + unresolvableHost + ":" + TestUtils.getInfluxPORT(true));
		});
	}

	@Test
  public void testInvalidUrlHandling(){
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      InfluxDBFactory.connect("@@@http://@@@");
    });
    
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      InfluxDBFactory.connect("http://@@@abc");
    });
  }

	@Test
	public void testBatchEnabledTwice() {
		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
		try{
			Assertions.assertThrows(IllegalStateException.class, () -> {
				this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
			});
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
		Assertions.assertTrue(influxDB.isBatchEnabled());
		influxDB.close();
		Assertions.assertFalse(influxDB.isBatchEnabled());
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

            Assertions.assertEquals(result.getResults().get(0).getSeries().size(), 3);
            Assertions.assertEquals("test1", result.getResults().get(0).getSeries().get(0).getTags().get("atag"));
            Assertions.assertEquals("test2", result.getResults().get(0).getSeries().get(1).getTags().get("atag"));
            Assertions.assertEquals("test3", result.getResults().get(0).getSeries().get(2).getTags().get("atag"));
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
            Assertions.assertFalse(influxDBForTestGzip.isGzipEnabled());
            influxDBForTestGzip.enableGzip();
            Assertions.assertTrue(influxDBForTestGzip.isGzipEnabled());
            influxDBForTestGzip.disableGzip();
            Assertions.assertFalse(influxDBForTestGzip.isGzipEnabled());
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

      Thread.sleep(2000);
      final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>();
      Query query = new Query("SELECT * FROM disk", dbName);
      this.influxDB.query(query, 2, new Consumer<QueryResult>() {
          @Override
          public void accept(QueryResult result) {
              queue.add(result);
          }});

      Thread.sleep(2000);
      this.influxDB.deleteDatabase(dbName);

      QueryResult result = queue.poll(20, TimeUnit.SECONDS);
      Assertions.assertNotNull(result);
      System.out.println(result);
      Assertions.assertEquals(2, result.getResults().get(0).getSeries().get(0).getValues().size());

      result = queue.poll(20, TimeUnit.SECONDS);
      Assertions.assertNotNull(result);
      System.out.println(result);
      Assertions.assertEquals(1, result.getResults().get(0).getSeries().get(0).getValues().size());
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
        Assertions.assertFalse(countDownLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test chunking on 0.13 and 1.0.
     * @throws InterruptedException
     */
    @Test()
    public void testChunkingOldVersion() throws InterruptedException {

        if (this.influxDB.version().startsWith("0.") || this.influxDB.version().startsWith("1.0")) {

            Assertions.assertThrows(RuntimeException.class, () -> {
            String dbName = "write_unittest_" + System.currentTimeMillis();
            Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
            this.influxDB.query(query, 10, new Consumer<QueryResult>() {
                @Override
                public void accept(QueryResult result) {
                }
			});
		});
        }
    }

	@Test
	public void testChunkingOnComplete() throws InterruptedException {
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

		CountDownLatch countDownLatch = new CountDownLatch(1);

		Thread.sleep(2000);
		Query query = new Query("SELECT * FROM disk", dbName);
		this.influxDB.query(query, 2, result -> {}, countDownLatch::countDown);

		Thread.sleep(2000);
		this.influxDB.deleteDatabase(dbName);

		boolean await = countDownLatch.await(10, TimeUnit.SECONDS);
		Assertions.assertTrue(await, "The onComplete action did not arrive!");
	}

	@Test
	public void testChunkingFailOnComplete() throws InterruptedException {
		if (this.influxDB.version().startsWith("0.") || this.influxDB.version().startsWith("1.0")) {
			// do not test version 0.13 and 1.0
			return;
		}
		String dbName = "write_unittest_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		Query query = new Query("UNKNOWN_QUERY", dbName);
		this.influxDB.query(query, 10, result -> {}, countDownLatch::countDown);
		this.influxDB.deleteDatabase(dbName);

		boolean await = countDownLatch.await(5, TimeUnit.SECONDS);
		Assertions.assertFalse(await, "The onComplete action arrive!");
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
            Assertions.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
        } finally {
            this.influxDB.deleteDatabase(dbName);
            this.influxDB.disableBatch();
        }
    }

    @Test
    public void testFlushThrowsIfBatchingIsNotEnabled() {
        Assertions.assertFalse(this.influxDB.isBatchEnabled());
        Assertions.assertThrows(IllegalStateException.class, () -> {
			this.influxDB.flush();
		});
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
		Assertions.assertNull(result.getError());
		List<List<Object>> retentionPolicies = result.getResults().get(0).getSeries().get(0).getValues();
		Assertions.assertTrue(retentionPolicies.get(1).contains("testRP1"));
		Assertions.assertTrue(retentionPolicies.get(2).contains("testRP2"));
		Assertions.assertTrue(retentionPolicies.get(3).contains("testRP3"));

		this.influxDB.dropRetentionPolicy("testRP1", dbName);
		this.influxDB.dropRetentionPolicy("testRP2", dbName);
		this.influxDB.dropRetentionPolicy("testRP3", dbName);

		result = this.influxDB.query(query);
		Assertions.assertNull(result.getError());
		retentionPolicies = result.getResults().get(0).getSeries().get(0).getValues();
		Assertions.assertTrue(retentionPolicies.size() == 1);
	}

	/**
	 * Test the implementation of {@link InfluxDB#isBatchEnabled() with consistency}.
	 */
	@Test
	public void testIsBatchEnabledWithConsistency() {
		Assertions.assertFalse(this.influxDB.isBatchEnabled());
		this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS, Executors.defaultThreadFactory(),
				(a, b) -> {
				}, InfluxDB.ConsistencyLevel.ALL);
		Assertions.assertTrue(this.influxDB.isBatchEnabled());
	}

	/**
   * Test initialize InfluxDBImpl with MessagePack format for InfluxDB versions before 1.4 will throw exception
   */
	@Test
	@EnabledIfEnvironmentVariable(named = "INFLUXDB_VERSION", matches = "1\\.3|1\\.2|1\\.1")
	public void testMessagePackOnOldDbVersion() {
	  Assertions.assertThrows(UnsupportedOperationException.class, () -> {
	    InfluxDB influxDB = TestUtils.connectToInfluxDB(ResponseFormat.MSGPACK);
	    influxDB.describeDatabases();
	  });
	}

  /**
   * test for issue #445
   * make sure reusing of OkHttpClient.Builder causes no error
   * @throws InterruptedException
   */
  @Test
  public void testIssue445() throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(100);

    final int maxCallables = 10_000;
    List<Callable<String>> callableList = new ArrayList<>(maxCallables);
    for (int i = 0; i < maxCallables; i++) {
      callableList.add(new Callable<String>() {
        @Override
        public String call() throws Exception {
          MyInfluxDBBean myBean = new MyInfluxDBBean();
          return myBean.connectAndDoNothing1();
        }
      });
    }
    executor.invokeAll(callableList);
    executor.shutdown();
    if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
      executor.shutdownNow();
    }
    Assertions.assertTrue(MyInfluxDBBean.OK);
    //assert that MyInfluxDBBean.OKHTTP_BUILDER stays untouched (no interceptor added)
    Assertions.assertTrue(MyInfluxDBBean.OKHTTP_BUILDER.interceptors().isEmpty());
  }

  private static final class MyInfluxDBBean {

    static final OkHttpClient.Builder OKHTTP_BUILDER = new OkHttpClient.Builder();
    static Boolean OK = true;
    static final String URL = "http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true);

    InfluxDB influxClient;

    String connectAndDoNothing1() {
      synchronized (OK) {
        if (!OK) {
          return null;
        }
      }
      try {
        influxClient = InfluxDBFactory.connect(URL, "admin", "admin", OKHTTP_BUILDER);
        influxClient.close();
      } catch (Exception e) {
        synchronized (OK) {
          if (OK) {
            OK = false;
          }
        }
      }
      return null;
    }
  }
}

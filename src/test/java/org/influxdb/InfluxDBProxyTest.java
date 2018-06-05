package org.influxdb;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
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
public class InfluxDBProxyTest {

	private InfluxDB influxDB;
	private final static String DATABASE = "testDb";

	/**
	 * Create a influxDB connection before all tests start.
	 *
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@BeforeEach
	public void setUp() throws InterruptedException, IOException {
		String url = "http://" + TestUtils.getProxyIP() + "/influxdb/";
		System.out.println("############################################################################################# ");
		System.out.println("#  Url: "+url);

		this.influxDB = InfluxDBFactory.connect(url, "admin", "admin");
		boolean influxDBstarted = false;
		do {
			Pong response;
			try {
				response = this.influxDB.ping();
				if (response.isGood()) {
					influxDBstarted = true;
				}
			} catch (Exception e) {
				// NOOP intentional
				e.printStackTrace();
			}
			Thread.sleep(100L);
		} while (!influxDBstarted);
		this.influxDB.setLogLevel(LogLevel.NONE);
		this.influxDB.createDatabase(DATABASE);
		System.out.println("#  Connected to InfluxDB Version: " + this.influxDB.version() + " using Proxy #");
		System.out.println("#############################################################################################");

	}
	
	/**
	 * delete UDP database after all tests end.
	 */
	@AfterEach
	public void cleanup(){
		this.influxDB.deleteDatabase(DATABASE);
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

        result = queue.poll(20, TimeUnit.SECONDS);
        Assertions.assertNotNull(result);
        System.out.println(result);
        Assertions.assertEquals("DONE", result.getError());
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
}

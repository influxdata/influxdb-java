package org.influxdb;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the InfluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class TicketTests {

	private InfluxDB influxDB;

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
		// String logs = CharStreams.toString(new InputStreamReader(containerLogsStream,
		// Charsets.UTF_8));
		System.out.println("##################################################################################");
		// System.out.println("Container Logs: \n" + logs);
		System.out.println("#  Connected to InfluxDB Version: " + this.influxDB.version() + " #");
		System.out.println("##################################################################################");
	}

	/**
	 * Test for ticket #38
	 *
	 */
	@Test
	public void testTicket38() {
		String dbName = "ticket38_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		Point point1 = Point
				.measurement("metric")
				.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
				.addField("value", 5.0)
				.tag("host", "host A")
				.tag("host", "host-B")
				.tag("host", "host-\"C")
				.tag("region", "region")
				.build();
		this.influxDB.write(dbName, TestUtils.defaultRetentionPolicy(this.influxDB.version()), point1);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test for ticket #39
	 *
	 */
	@Test
	public void testTicket39() {
		String dbName = "ticket39_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		BatchPoints batchPoints = BatchPoints
				.database(dbName)
				.tag("async", "true")
				.retentionPolicy(TestUtils.defaultRetentionPolicy(this.influxDB.version()))
				.consistency(InfluxDB.ConsistencyLevel.ALL)
				.build();
		Point.Builder builder = Point.measurement("my_type");
		builder.addField("my_field", "string_value");
		Point point = builder.build();
		batchPoints.point(point);
		this.influxDB.write(batchPoints);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test for ticket #40
	 */
	@Test
	public void testTicket40() {
		String dbName = "ticket40_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		this.influxDB.enableBatch(100, 100, TimeUnit.MICROSECONDS);
		for (int i = 0; i < 1000; i++) {
			Point point = Point.measurement("cpu").addField("idle", 99.0).build();
			this.influxDB.write(dbName, TestUtils.defaultRetentionPolicy(this.influxDB.version()), point);
		}
		this.influxDB.deleteDatabase(dbName);
	}

    /**
     * Test for ticket #256
     *
     * @throws Exception
     */
    @Test
    public void testTicket256() throws Exception {
        String dbName = "ticket256_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);

        // Make a nanosecond accurate timestamp
        long millis = System.currentTimeMillis();
        long innano = millis * 1000000 + 1;
        // Use a long value that will overflow a Double
        long lval = 1485370052974000001L;

        // Insert a point using the large long value
        BatchPoints batchPoints = BatchPoints.database(dbName).tag("async", "true").retentionPolicy("autogen")
                .consistency(ConsistencyLevel.ALL).build();
        Point point = Point.measurement("testProblems").time(innano, TimeUnit.NANOSECONDS)
                .addField("long", lval).build();

        batchPoints.point(point);
        influxDB.write(batchPoints);

        // Query the point back out
        Query query = new Query("SELECT * FROM testProblems ORDER BY time DESC LIMIT 1", dbName);
        QueryResult result = influxDB.query(query);
        // We are done with the database
        this.influxDB.deleteDatabase(dbName);

        // Extract the columns and value for the first (only) series
        Series series0 = result.getResults().get(0).getSeries().get(0);
        List<String> cols = series0.getColumns();
        List<Object> val0 = series0.getValues().get(0);

        // Extract the object associated with the long field
        Object outlval_obj = val0.get(cols.indexOf("long"));
        long outlval;

        // Convert from Double (which we expect) or try to convert from String
        // (something is wrong)
        if (!(outlval_obj instanceof Double)) {
            System.err.println("Got unexpected type output numeric (not Double), trying conversion from string: "
                    + outlval_obj.getClass().getName());
            outlval = Long.valueOf(outlval_obj.toString());
        } else {
            outlval = ((Double) outlval_obj).longValue();
        }

        // Compare the value we got back with the value we wrote in
        if (outlval != lval) {
            fail("Got bad lval back as [" + (outlval_obj) + "] -> " + outlval + " != " + lval);
        }

    }

    /**
     * Test for ticket #256 timestamp specific problems
     *
     * @throws Exception
     */
    @Test
    public void testTicket256_timestamp() throws Exception {
        String dbName = "ticket256_" + System.currentTimeMillis();
        this.influxDB.createDatabase(dbName);

        // Make a nanosecond accurate timestamp
        long millis = System.currentTimeMillis();
        long innano = millis * 1000000 + 1;
        Instant instamp = Instant.ofEpochMilli(millis).plusNanos(1);

        // write a point to the database
        BatchPoints batchPoints = BatchPoints.database(dbName).tag("async", "true").retentionPolicy("autogen")
                .consistency(ConsistencyLevel.ALL).build();
        Point point = Point.measurement("testProblems").time(innano, TimeUnit.NANOSECONDS).addField("double", 3.14)
                .build();

        batchPoints.point(point);
        influxDB.write(batchPoints);

        Query query = new Query("SELECT * FROM testProblems ORDER BY time DESC LIMIT 1", dbName);
        // Query the point using rfc3339 date time format
        QueryResult result = influxDB.query(query);

        Series series0 = result.getResults().get(0).getSeries().get(0);
        List<String> cols = series0.getColumns();
        List<Object> val0 = series0.getValues().get(0);

        String tsstr = (String) val0.get(cols.indexOf("time"));

        // Query the poing using long nanoseconds date time format
        result = influxDB.query(query, TimeUnit.NANOSECONDS);

        series0 = result.getResults().get(0).getSeries().get(0);
        cols = series0.getColumns();
        val0 = series0.getValues().get(0);

        long outnano;
        Object tsnano_obj = val0.get(cols.indexOf("time"));
        // Convert from Double (which we expect) or try to convert from String
        // (something is wrong)
        if (!(tsnano_obj instanceof Double)) {
            System.err.println("Got unexpected type output numeric (not Double), trying conversion from string: "
                    + tsnano_obj.getClass().getName());
            outnano = Long.valueOf(tsnano_obj.toString());
        } else {
            outnano = ((Double) tsnano_obj).longValue();
        }

        // We are done with the database
        this.influxDB.deleteDatabase(dbName);

        // Convert the string timestamp and check against the original timestamp
        Instant outstamp = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(tsstr));

        // This should work...
        if (!outstamp.equals(instamp)) {
            throw new Exception(
                    "Got bad timestamp value back as string [" + tsstr + "] -> " + outstamp + " != " + instamp);
        }

        // This will fail if numeric conversion fails
        // Convert the numeric timestamp and compare against the original timestamp
        long outmillis = outnano / 1000000;
        outstamp = Instant.ofEpochMilli(outmillis).plusNanos(outnano - (outmillis * 1000000));
        if ((outnano != innano) || (!outstamp.equals(instamp))) {
            fail("Got bad long nanos back as double [" + tsnano_obj + "] -> " + outnano + " ?= " + innano
                    + "\nAND/OR Got bad timestamp back as double [" + tsnano_obj + "] -> " + outstamp + " ?= "
                    + instamp);
        }

    }

}

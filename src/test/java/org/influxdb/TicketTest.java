
package org.influxdb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.impl.InfluxDBResultMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
/**
 * Test the InfluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
@DisplayName("Test for github issues")
@RunWith(JUnitPlatform.class)
public class TicketTest {

    private final InfluxDBResultMapper mapper = new InfluxDBResultMapper();

	private InfluxDB influxDB;

	/**
	 * Create a influxDB connection before all tests start.
	 *
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@BeforeEach
	public void setUp() throws InterruptedException, IOException {
		this.influxDB = TestUtils.connectToInfluxDB();
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
	 * Test for ticket #303
	 *
	 */
	@Test
	public void testTicket303() {
		String dbName = "ticket303_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		
                
                Date rundate1 = new Date() ; 
                long rundate1Sec = rundate1.getTime() / 1000;
       
              
        
          Point point1 = Point
                            .measurement("TestSlash")
                            .time(rundate1Sec, TimeUnit.SECONDS)
                            .tag("precision", "Second")                       
                            .addField("MultipleSlash" ,  "echo \\\".ll 12.0i\\\";")                            
                            .build(); 
		this.influxDB.write(dbName, TestUtils.defaultRetentionPolicy(this.influxDB.version()), point1);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
     * Test for ticket #440
     */
    @Test
    public void testTicket440() {
        String dbName = "ticket440_" + System.currentTimeMillis();
        influxDB.createDatabase(dbName);

        long tsEgress = 1523264395509072018L;
        Instant now = Instant.now();

        Point point = Point.measurement("latency_1")
                            .time(now.toEpochMilli(), TimeUnit.MILLISECONDS)
                            .tag("msg_type", "1")                       
                            .addField("ts_egr", tsEgress)
                            .addField("ts_egr_long", tsEgress)
                            .addField("pi", Math.PI)
                            .build();
        influxDB.write(dbName, TestUtils.defaultRetentionPolicy(influxDB.version()), point);

        Query query = new Query("SELECT * FROM latency_1", dbName);
        Latency latency = mapper.toPOJO(influxDB.query(query), Latency.class).get(0);
        assertEquals(tsEgress, latency.tsEgress);
        
        influxDB.deleteDatabase(dbName);
    }

    @Measurement(name = "latency_1")
    public static class Latency {
        @Column(name = "time")
        Instant time;

        @Column(name = "msg_type")
        String msgType;

        @Column(name = "ts_egr")
        long tsEgress;
        
        @Column(name = "ts_egr_long")
        Long tsEgressLong;

        @Column(name = "pi")
        double pi;

        @Override
        public String toString() {
            return "Latency [time=" + time + ", msgType=" + msgType + ", tsEgress=" + tsEgress + ", tsEgressLong="
                    + tsEgressLong + ", pi=" + pi + "]";
        }
    }
}
package org.influxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * Test the InfluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
@RunWith(JUnitPlatform.class)
public class UDPInfluxDBTest {

    private InfluxDB influxDB;
    private final static int UDP_PORT = 8089;
    private final static String UDP_DATABASE = "udp";

     /**
     * Create a influxDB connection before all tests start.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @BeforeEach
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
    @AfterEach
    public void cleanup() {
        this.influxDB.deleteDatabase(UDP_DATABASE);
    }
    
    /**
     * Test the implementation of {@link InfluxDB#write(int, Point)}'s sync
     * support.
     */
    @Test
    public void testSyncWritePointThroughUDP() throws InterruptedException {
        this.influxDB.disableBatch();
        String measurement = TestUtils.getRandomMeasurement();
        Point point = Point.measurement(measurement).tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
        this.influxDB.write(UDP_PORT, point);
        Thread.sleep(2000);
        Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
        QueryResult result = this.influxDB.query(query);
        Assertions.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
    }

    /**
     * Test the implementation of {@link InfluxDB#write(int, Point)}'s async
     * support.
     */
    @Test
    public void testAsyncWritePointThroughUDP() throws InterruptedException {
        this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
        try {
            Assertions.assertTrue(this.influxDB.isBatchEnabled());
            String measurement = TestUtils.getRandomMeasurement();
            Point point = Point.measurement(measurement).tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
            this.influxDB.write(UDP_PORT, point);
            Thread.sleep(2000);
            Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
            QueryResult result = this.influxDB.query(query);
            Assertions.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
        } finally {
            this.influxDB.disableBatch();
        }
    }

    /**
     * Test the implementation of {@link InfluxDB#write(int, Point)}'s async
     * support.
     */
    @Test
    public void testAsyncWritePointThroughUDPFail() {
        this.influxDB.enableBatch(1, 1, TimeUnit.SECONDS);
        try {
            Assertions.assertTrue(this.influxDB.isBatchEnabled());
            String measurement = TestUtils.getRandomMeasurement();
            Point point = Point.measurement(measurement).tag("atag", "test").addField("used", 80L).addField("free", 1L).build();
            Thread.currentThread().interrupt();
            Assertions.assertThrows(RuntimeException.class, () -> {
                this.influxDB.write(UDP_PORT, point);
            });
        } finally {
            this.influxDB.disableBatch();
        }
    }

    /**
     * Test writing to the database using string protocol through UDP.
     */
    @Test
    public void testWriteStringDataThroughUDP() throws InterruptedException {
        String measurement = TestUtils.getRandomMeasurement();
        this.influxDB.write(UDP_PORT, measurement + ",atag=test idle=90,usertime=9,system=1");
        //write with UDP may be executed on server after query with HTTP. so sleep 2s to handle this case
        Thread.sleep(2000);
        Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
        QueryResult result = this.influxDB.query(query);
        Assertions.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
    }

   

    /**
     * When batch of points' size is over UDP limit, the expected exception is
     * java.lang.RuntimeException: java.net.SocketException: The message is
     * larger than the maximum supported by the underlying transport: Datagram
     * send failed
     *
     * @throws Exception
     */
    @Test
    public void testWriteMultipleStringDataLinesOverUDPLimit() throws Exception {
        //prepare data
        List<String> lineProtocols = new ArrayList<String>();
        int i = 0;
        int length = 0;
        while (true) {
            Point point = Point.measurement("udp_single_poit").addField("v", i).build();
            String lineProtocol = point.lineProtocol();
            length += (lineProtocol.getBytes("utf-8")).length;
            lineProtocols.add(lineProtocol);
            if (length > 65535) {
                break;
            }
        }
        //write batch of string which size is over 64K
        Assertions.assertThrows(RuntimeException.class, () -> {
            this.influxDB.write(UDP_PORT, lineProtocols);
        });
    }

    /**
     * Test writing multiple records to the database using string protocol
     * through UDP.
     */
    @Test
    public void testWriteMultipleStringDataThroughUDP() throws InterruptedException {
        String measurement = TestUtils.getRandomMeasurement();
        this.influxDB.write(UDP_PORT, measurement + ",atag=test1 idle=100,usertime=10,system=1\n"
                + measurement + ",atag=test2 idle=200,usertime=20,system=2\n"
                + measurement + ",atag=test3 idle=300,usertime=30,system=3");
        Thread.sleep(2000);
        Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
        QueryResult result = this.influxDB.query(query);

        Assertions.assertEquals(3, result.getResults().get(0).getSeries().size());
        Assertions.assertEquals("test1", result.getResults().get(0).getSeries().get(0).getTags().get("atag"));
        Assertions.assertEquals("test2", result.getResults().get(0).getSeries().get(1).getTags().get("atag"));
        Assertions.assertEquals("test3", result.getResults().get(0).getSeries().get(2).getTags().get("atag"));
    }

    /**
     * Test writing multiple separate records to the database using string
     * protocol through UDP.
     */
    @Test
    public void testWriteMultipleStringDataLinesThroughUDP() throws InterruptedException {
        String measurement = TestUtils.getRandomMeasurement();
        this.influxDB.write(UDP_PORT, Arrays.asList(
                measurement + ",atag=test1 idle=100,usertime=10,system=1",
                measurement + ",atag=test2 idle=200,usertime=20,system=2",
                measurement + ",atag=test3 idle=300,usertime=30,system=3"
        ));
        Thread.sleep(2000);
        Query query = new Query("SELECT * FROM " + measurement + " GROUP BY *", UDP_DATABASE);
        QueryResult result = this.influxDB.query(query);

        Assertions.assertEquals(3, result.getResults().get(0).getSeries().size());
        Assertions.assertEquals("test1", result.getResults().get(0).getSeries().get(0).getTags().get("atag"));
        Assertions.assertEquals("test2", result.getResults().get(0).getSeries().get(1).getTags().get("atag"));
        Assertions.assertEquals("test3", result.getResults().get(0).getSeries().get(2).getTags().get("atag"));
    }
}

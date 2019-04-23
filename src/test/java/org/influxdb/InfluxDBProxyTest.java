package org.influxdb;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Point;
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
 * @author hoan.le [at] bonitoo.io
 *
 */
@RunWith(JUnitPlatform.class)
public class InfluxDBProxyTest {
  private InfluxDB influxDB;
  private static final String TEST_DB = "InfluxDBProxyTest_db";
  private static final String UDP_DB = "udp";

  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    influxDB = TestUtils.connectToInfluxDB(TestUtils.getProxyApiUrl());
  }

  /**
   * delete database after all tests end.
   */
  @AfterEach
  public void cleanup(){
    influxDB.close();
  }

  @Test
  public void testWriteSomePointThroughTcpProxy() {
    influxDB.query(new Query("CREATE DATABASE " + TEST_DB));;
    influxDB.setDatabase(TEST_DB);

    for(int i = 0; i < 20; i++) {
      Point point = Point.measurement("weather")
          .time(i,TimeUnit.HOURS)
          .addField("temperature", (double) i)
          .addField("humidity", (double) (i) * 1.1)
          .addField("uv_index", "moderate").build();
      influxDB.write(point);
    }

    QueryResult result = influxDB.query(new Query("select * from weather", TEST_DB));
    //check points written already to DB
    Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());

    influxDB.deleteDatabase(TEST_DB);
  }

  @Test
  public void testWriteSomePointThroughUdpProxy() throws InterruptedException {
    influxDB.query(new Query("CREATE DATABASE " + UDP_DB));
    influxDB.setDatabase(UDP_DB);

    int proxyUdpPort = Integer.parseInt(TestUtils.getProxyUdpPort());
    for(int i = 0; i < 20; i++) {
      Point point = Point.measurement("weather")
          .time(i,TimeUnit.HOURS)
          .addField("temperature", (double) i)
          .addField("humidity", (double) (i) * 1.1)
          .addField("uv_index", "moderate").build();
      influxDB.write(proxyUdpPort, point);
    }

    Thread.sleep(2000);
    QueryResult result = influxDB.query(new Query("select * from weather", UDP_DB));
    //check points written already to DB
    Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());

    influxDB.deleteDatabase(UDP_DB);
  }

}

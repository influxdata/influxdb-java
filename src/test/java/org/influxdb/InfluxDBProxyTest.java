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
  private String db = "udp";

  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    this.influxDB = TestUtils.connectToInfluxDB(TestUtils.getProxyApiUrl());
    this.influxDB.createDatabase(db);
    influxDB.setDatabase(db);
  }

  /**
   * delete database after all tests end.
   */
  @AfterEach
  public void cleanup(){
    this.influxDB.deleteDatabase(db);
  }
  
  @Test
  public void testWriteSomePointThroughTcpProxy() {
    for(int i = 0; i < 20; i++) {
      Point point = Point.measurement("weather")
          .time(i,TimeUnit.HOURS)
          .addField("temperature", (double) i)
          .addField("humidity", (double) (i) * 1.1)
          .addField("uv_index", "moderate").build();
      influxDB.write(point);
    }

    QueryResult result = influxDB.query(new Query("select * from weather", db));
    //check points written already to DB 
    Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    
  }
  
  @Test
  public void testWriteSomePointThroughUdpProxy() throws InterruptedException {
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
    QueryResult result = influxDB.query(new Query("select * from weather", db));
    //check points written already to DB 
    Assertions.assertEquals(20, result.getResults().get(0).getSeries().get(0).getValues().size());
    
  }

}

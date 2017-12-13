package org.influxdb;

import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@RunWith(JUnitPlatform.class)
public class BatchOptionsTest {

  private InfluxDB influxDB;

  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    this.influxDB = TestUtils.connectToInfluxDB();
  }

  /**
   * Test the implementation of {@link InfluxDB#enableBatch(int, int, TimeUnit, ThreadFactory)}.
   */
  @Test
  public void testBatchEnabledWithDefaultSettings() {
    try {
      this.influxDB.enableBatch();

    }
    finally {
      this.influxDB.disableBatch();
    }
  }

  /**
   * Test the implementation of {@link BatchOptions#actions(int)} }.
   */
  @Test
  public void testActionsSetting() throws InterruptedException {
    String dbName = "write_unittest_" + System.currentTimeMillis();
    try {
      BatchOptions options = BatchOptions.DEFAULTS.actions(3);
      Assertions.assertEquals(3, options.getActions());
      this.influxDB.enableBatch(options);
      this.influxDB.createDatabase(dbName);
      this.influxDB.setDatabase(dbName);
      for (int j = 0; j < 5; j++) {
        Point point = Point.measurement("cpu")
                .time(j,TimeUnit.MILLISECONDS)
                .addField("idle", (double) j)
                .addField("user", 2.0 * j)
                .addField("system", 3.0 * j).build();
        this.influxDB.write(point);
      }
      Thread.sleep(500);
      QueryResult result=influxDB.query(new Query("select * from cpu", dbName));
      Assertions.assertEquals(3, result.getResults().get(0).getSeries().get(0).getValues().size());
    }
    finally {
      this.influxDB.disableBatch();
      this.influxDB.deleteDatabase(dbName);
    }
  }

}

package org.influxdb;

import java.io.IOException;

import org.influxdb.InfluxDB.ResponseFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * Test the BatchOptions with MessagePack format
 *
 * @author hoan.le [at] bonitoo.io
 *
 */
@RunWith(JUnitPlatform.class)
@EnabledIfEnvironmentVariable(named = "INFLUXDB_VERSION", matches = "1\\.6|1\\.5|1\\.4")
public class MessagePackBatchOptionsTest extends BatchOptionsTest {

  @Override
  @BeforeEach
  public void setUp() throws InterruptedException, IOException {
    influxDB = TestUtils.connectToInfluxDB(ResponseFormat.MSGPACK);
  }
}

package org.influxdb;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.influxdb.dto.Pong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import net.jcip.annotations.NotThreadSafe;

/**
 * Test the InfluxDBimpl log level setting from system property.
 *
 * @author hoan.le [at] bonitoo.io
 *
 */
@RunWith(JUnitPlatform.class)
@NotThreadSafe
public class InfluxDBLogLevelTest {
  @Test
  public void testLogLevelProperties() {
    String oldLogLevel = System.getProperty(InfluxDB.LOG_LEVEL_PROP);
    try {
      List<String> logLevels = Arrays.asList(null, "NONE", "BASIC", "HEADERS", "FULL", "abc");
      logLevels.forEach(logLevel -> {
        System.out.println("LogLevel = " + logLevel);
        Optional.ofNullable(logLevel).ifPresent(value -> {
          System.setProperty(InfluxDB.LOG_LEVEL_PROP, value);
        });
        
        try {
          InfluxDB influxDB = TestUtils.connectToInfluxDB();
          Pong result = influxDB.ping();
          Assertions.assertNotNull(result);
          influxDB.close();
        } catch (Exception e) {
          Assertions.fail(e);
        }
      });
    } finally {
      if (oldLogLevel == null) {
        System.clearProperty(InfluxDB.LOG_LEVEL_PROP);
      } else {
        System.setProperty(InfluxDB.LOG_LEVEL_PROP, oldLogLevel);
      }
    }

  }
}

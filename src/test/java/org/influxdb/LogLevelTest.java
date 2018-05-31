package org.influxdb;

import java.util.HashMap;
import java.util.Map;

import org.influxdb.InfluxDB.LogLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * Test the InfluxDBimpl log level setting from system property.
 *
 * @author hoan.le [at] bonitoo.io
 *
 */
@RunWith(JUnitPlatform.class)
public class LogLevelTest {
  @Test
  public void testParseLogLevel() {
    Map<String, LogLevel> logLevelMap = new HashMap<>();
    logLevelMap.put(null, LogLevel.NONE);
    logLevelMap.put("NONE", LogLevel.NONE);
    logLevelMap.put("BASIC", LogLevel.BASIC);
    logLevelMap.put("HEADERS", LogLevel.HEADERS);
    logLevelMap.put("FULL", LogLevel.FULL);
    logLevelMap.put("abc", LogLevel.NONE);
    logLevelMap.forEach((value, logLevel) -> {
      Assertions.assertEquals(LogLevel.parseLogLevel(value), logLevel);
    });
  }
}

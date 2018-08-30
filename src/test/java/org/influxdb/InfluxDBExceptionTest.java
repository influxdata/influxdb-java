package org.influxdb;

import org.influxdb.InfluxDBException.DatabaseNotFoundException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * Test cases for InfluxDBException
 *
 * @author hoan.le [at] bonitoo.io
 *
 */

@RunWith(JUnitPlatform.class)
public class InfluxDBExceptionTest {

  @Test
  public void testBuildExceptionForMessagePackErrorState() {
    DatabaseNotFoundException dbex = (DatabaseNotFoundException) InfluxDBException
        .buildExceptionForErrorState(InfluxDBExceptionTest.class.getResourceAsStream("msgpack_errorBody.bin"));
    
    Assertions.assertEquals("database not found: \"abc\"", dbex.getMessage());

    InfluxDBException ex = InfluxDBException.buildExceptionForErrorState(InfluxDBExceptionTest.class.getResourceAsStream("invalid_msgpack_errorBody.bin"));
    Assertions.assertTrue(ex.getCause() instanceof ClassCastException);
    
  }
}

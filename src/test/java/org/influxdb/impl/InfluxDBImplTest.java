package org.influxdb.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;

import org.influxdb.InfluxDB;
import org.influxdb.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import okhttp3.OkHttpClient;

public class InfluxDBImplTest {

  private InfluxDB influxDB;

  @BeforeEach
  public void setUp() throws Exception {
    this.influxDB = TestUtils.connectToInfluxDB();
  }
  
  @AfterEach
  public void cleanup() {
    influxDB.close();
  }

  @Test
  public void closeOkHttpClient() throws Exception { 
    OkHttpClient client = getPrivateField(influxDB, "client");
    assertFalse(client.dispatcher().executorService().isShutdown());
    assertFalse(client.connectionPool().connectionCount() == 0);
    influxDB.close();
    assertTrue(client.dispatcher().executorService().isShutdown());
    assertTrue(client.connectionPool().connectionCount() == 0);
  }

  @SuppressWarnings("unchecked")
  static <T> T getPrivateField(final Object obj, final String name)
      throws Exception {
    Field field = obj.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(obj);
  }

}

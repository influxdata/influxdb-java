package org.influxdb;

import okhttp3.OkHttpClient;

import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.dto.Pong;

import java.io.IOException;
import java.util.Map;

public class TestUtils {

  private static String getEnv(String name, String defaultValue) {
    Map<String, String> getenv = System.getenv();

    if (getenv.containsKey(name)) {
      return getenv.get(name);
    } else {
      return defaultValue;
    }
  }
  
  public static String getInfluxIP() {
    return getEnv("INFLUXDB_IP", "127.0.0.1");
  }
  
  public static String getRandomMeasurement() {
    return "measurement_" + System.nanoTime();
  }
  
  public static String getInfluxPORT(boolean apiPort) {
    if(apiPort) {    
      return getEnv("INFLUXDB_PORT_API", "8086");
    }
    else {
      return getEnv("INFLUXDB_PORT_COLLECTD", "8096");
    }      
  }

  public static String getProxyApiUrl() {
    return getEnv("PROXY_API_URL", "http://127.0.0.1:8086/");
  }

  public static String getProxyUdpPort() {
    return getEnv("PROXY_UDP_PORT", "8089");
  }

  public static String defaultRetentionPolicy(String version) {
    if (version.startsWith("0.") ) {
      return "default";
    } else {
      return "autogen";
    }
  }

  public static InfluxDB connectToInfluxDB() throws InterruptedException, IOException {
    return connectToInfluxDB(null, null, ResponseFormat.JSON);
  }

  public static InfluxDB connectToInfluxDB(ResponseFormat responseFormat) throws InterruptedException, IOException {
    return connectToInfluxDB(null, null, responseFormat);
  }
  public static InfluxDB connectToInfluxDB(String apiUrl) throws InterruptedException, IOException {
    return connectToInfluxDB(new OkHttpClient.Builder(), apiUrl, ResponseFormat.JSON);
  }
  
  public static InfluxDB connectToInfluxDB(final OkHttpClient.Builder client, String apiUrl,
      ResponseFormat responseFormat) throws InterruptedException, IOException {
    OkHttpClient.Builder clientToUse;
    if (client == null) {
      clientToUse = new OkHttpClient.Builder();
    } else {
      clientToUse = client;
    }
    String apiUrlToUse; 
    if (apiUrl == null) {
      apiUrlToUse = "http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true);
    } else {
      apiUrlToUse = apiUrl;
    }
    InfluxDB influxDB = InfluxDBFactory.connect(apiUrlToUse, "admin", "admin", clientToUse, responseFormat);
    boolean influxDBstarted = false;
    do {
      Pong response;
      try {
        response = influxDB.ping();
        if (response.isGood()) {
          influxDBstarted = true;
        }
      } catch (Exception e) {
        // NOOP intentional
        e.printStackTrace();
      }
      Thread.sleep(100L);
    } while (!influxDBstarted);
    influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
    System.out.println("##################################################################################");
    System.out.println("#  Connected to InfluxDB Version: " + influxDB.version() + " #");
    System.out.println("##################################################################################");
    return influxDB;
  }
}

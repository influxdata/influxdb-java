package org.influxdb;

import okhttp3.OkHttpClient;
import org.influxdb.dto.Pong;

import java.io.IOException;
import java.util.Map;

public class TestUtils {

    public static String getInfluxURL() {
        String ip = "http://127.0.0.1:8086";

        Map<String, String> getenv = System.getenv();
        if (getenv.containsKey("INFLUXDB_API_URL")) {
            ip = getenv.get("INFLUXDB_API_URL");
        }
        return ip;
    }
    
    
    public static String getRandomMeasurement() {
        return "measurement_" + System.nanoTime();
    }
    
    public static String defaultRetentionPolicy(String version) {
        if (version.startsWith("0.")) {
            return "default";
        } else {
            return "autogen";
        }
    }

  public static InfluxDB connectToInfluxDB() throws InterruptedException, IOException {
    return connectToInfluxDB(null);
  }

	public static InfluxDB connectToInfluxDB( final OkHttpClient.Builder client) throws InterruptedException, IOException {
    OkHttpClient.Builder clientToUse;
    if (client == null) {
      clientToUse = new OkHttpClient.Builder();
    } else {
      clientToUse = client;
    }
    InfluxDB influxDB = InfluxDBFactory.connect(
            "http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true),
            "admin", "admin", clientToUse);
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

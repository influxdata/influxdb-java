package org.influxdb;

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

}

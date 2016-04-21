package org.influxdb;

import java.util.Map;

public class TestUtils {

	public static String getInfluxIP() {
		String ip = "127.0.0.1";
		
		Map<String, String> getenv = System.getenv();
		if (getenv.containsKey("INFLUXDB_IP")) {
			ip = getenv.get("INFLUXDB_IP");
		}
		
		return ip;
	}

}

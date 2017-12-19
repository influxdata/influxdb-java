package org.influxdb;

import okhttp3.OkHttpClient;
import org.influxdb.dto.Pong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * Test the InfluxDB Factory API.
 *
 * @author fujian1115 [at] gmail.com
 *
 */
@RunWith(JUnitPlatform.class)
public class InfluxDBFactoryTest {

	/**
	 * Test for a {@link InfluxDBFactory #connect(String)}.
	 */
	@Test
	public void testCreateInfluxDBInstanceWithoutUserNameAndPassword() {
		InfluxDB influxDB = InfluxDBFactory.connect(TestUtils.getInfluxURL());
		verifyInfluxDBInstance(influxDB);
	}

	private void verifyInfluxDBInstance(InfluxDB influxDB) {
		Assertions.assertNotNull(influxDB);
		Pong pong = influxDB.ping();
		Assertions.assertNotNull(pong);
		Assertions.assertNotEquals(pong.getVersion(), "unknown");
	}

	/**
	 * Test for a {@link InfluxDBFactory #connect(String, okhttp3.OkHttpClient.Builder)}.
	 */
	@Test
	public void testCreateInfluxDBInstanceWithClientAndWithoutUserNameAndPassword() {
		InfluxDB influxDB = InfluxDBFactory.connect(TestUtils.getInfluxURL(), new OkHttpClient.Builder());
		verifyInfluxDBInstance(influxDB);
	}

	@Test
	public void testShouldThrowIllegalArgumentWithInvalidUrl() {
		Assertions.assertThrows(IllegalArgumentException.class,() -> {
			 InfluxDBFactory.connect("invalidUrl");
		});
	}
}

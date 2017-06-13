package org.influxdb;

import org.influxdb.dto.Pong;
import org.junit.Assert;
import org.junit.Test;

import okhttp3.OkHttpClient;

/**
 * Test the InfluxDB Factory API.
 *
 * @author fujian1115 [at] gmail.com
 *
 */
public class InfluxDBFactoryTest {
	
	/**
	 * Test for a {@link InfluxDBFactory #connect(String)}.
	 */
	@Test
	public void testCreateInfluxDBInstanceWithoutUserNameAndPassword() {
		InfluxDB influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true));
		verifyInfluxDBInstance(influxDB);
	}

	private void verifyInfluxDBInstance(InfluxDB influxDB) {
		Assert.assertNotNull(influxDB);
		Pong pong = influxDB.ping();
		Assert.assertNotNull(pong);
		Assert.assertNotEquals(pong.getVersion(), "unknown");
	}

	/**
	 * Test for a {@link InfluxDBFactory #connect(String, okhttp3.OkHttpClient.Builder)}.
	 */
	@Test
	public void testCreateInfluxDBInstanceWithClientAndWithoutUserNameAndPassword() {
		InfluxDB influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true), new OkHttpClient.Builder());
		verifyInfluxDBInstance(influxDB);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentWithInvalidUrl() {
		InfluxDBFactory.connect("invalidUrl");
	}
}

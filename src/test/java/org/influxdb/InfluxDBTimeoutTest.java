package org.influxdb;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Query;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import retrofit.RetrofitError;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

/**
 * Test the timeout settings.
 */
public class InfluxDBTimeoutTest {
	private static final int WIREMOCK_PORT = 8080;

	private WireMockServer wireMockServer;

	@BeforeClass
	public void setUp() {
		wireMockServer = new WireMockServer(wireMockConfig().port(WIREMOCK_PORT));
		wireMockServer.start();
		WireMock.addRequestProcessingDelay(2 * 1000);
		WireMock.configureFor(WIREMOCK_PORT);

		stubFor(get(urlPathEqualTo("/query")).willReturn(
				aResponse()
						.withStatus(200)
						.withHeader("Content-Type", "application/json")
						.withBody("{}")));
	}

	@AfterClass
	public void tearDown() {
		if (wireMockServer != null) {
			wireMockServer.stop();
		}
	}

	@Test(timeOut = 3 * 1000, expectedExceptions = RetrofitError.class, expectedExceptionsMessageRegExp = "connect timed out")
	public void testConnectTimeout() {
		// connect to any non-routable IP address will result in a connect timeout
		InfluxDB influxDB = InfluxDBFactory.connect("http://10.0.0.0:" + WIREMOCK_PORT, "user", "password");
		influxDB.setConnectTimeout(1, TimeUnit.SECONDS);

		influxDB.query(new Query("SELECT value FROM cpu", "test_db"));
	}

	@Test(timeOut = 3 * 1000, expectedExceptions = RetrofitError.class, expectedExceptionsMessageRegExp = "timeout")
	public void testReadTimeout() {
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:" + WIREMOCK_PORT, "user", "password");
		influxDB.setReadTimeout(1, TimeUnit.SECONDS);

		influxDB.query(new Query("SELECT value FROM cpu", "test_db"));
	}

}

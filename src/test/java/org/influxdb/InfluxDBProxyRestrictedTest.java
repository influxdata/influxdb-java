package org.influxdb;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBImpl;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Test the InfluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
@RunWith(JUnitPlatform.class)
public class InfluxDBProxyRestrictedTest {


	/**
	 * Test for a ping.
	 */
	@Test
	public void testPing() {
		String url = "http://" + TestUtils.getProxyIP() + "/influxdb/";
		InfluxDB db = InfluxDBFactory.connect(url, "user", "wrongPassword");
		try {
			Pong result = db.ping();
			Assert.fail("Exception must be thrown.");
		} catch (Exception ex){

		}

	}
}

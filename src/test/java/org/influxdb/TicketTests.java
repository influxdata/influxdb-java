package org.influxdb;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;

/**
 * Test the InfluxDB API.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
@Test
public class TicketTests {

	private InfluxDB influxDB;
	private DockerClient dockerClient;
	private CreateContainerResponse container;

	/**
	 * Create a influxDB connection before all tests start.
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@BeforeClass
	public void setUp() throws InterruptedException, IOException {
		// Disable logging for the DockerClient.
		Logger.getLogger("com.sun.jersey").setLevel(Level.OFF);
		DockerClientConfig config = DockerClientConfig
				.createDefaultConfigBuilder()
				.withVersion("1.16")
				.withUri("tcp://" + TestUtils.getInfluxIP() + ":4243")
				.withUsername("roott")
				.withPassword("root")
				.build();
		this.dockerClient = DockerClientBuilder.getInstance(config).build();
		String ip = "192.168.59.103";
		this.influxDB = InfluxDBFactory.connect("http://" + ip + ":8086", "root", "root");
		boolean influxDBstarted = false;
		do {
			Pong response;
			try {
				response = this.influxDB.ping();
				System.out.println(response);
				if (!response.getVersion().equalsIgnoreCase("unknown")) {
					influxDBstarted = true;
				}
			} catch (Exception e) {
				// NOOP intentional
				e.printStackTrace();
			}
			Thread.sleep(100L);
		} while (!influxDBstarted);
		this.influxDB.setLogLevel(LogLevel.FULL);
		// String logs = CharStreams.toString(new InputStreamReader(containerLogsStream,
		// Charsets.UTF_8));
		System.out.println("##################################################################################");
		// System.out.println("Container Logs: \n" + logs);
		System.out.println("#  Connected to InfluxDB Version: " + this.influxDB.version() + " #");
		System.out.println("##################################################################################");
	}

	/**
	 * Ensure all Databases created get dropped afterwards.
	 */
	@AfterClass
	public void tearDown() {
		System.out.println("Kill the Docker container");
		// this.dockerClient.killContainerCmd(this.container.getId()).exec();
	}

	/**
	 * Test for ticket #38
	 *
	 */
	@Test(enabled = true)
	public void testTicket38() {
		String dbName = "ticket38_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		Point point1 = Point
				.measurement("metric")
				.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
				.addField("value", 5.0)
				.tag("host", "host A")
				.tag("host", "host-B")
				.tag("host", "host-\"C")
				.tag("region", "region")
				.build();
		this.influxDB.write(dbName, "default", ConsistencyLevel.ONE, point1);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test for ticket #39
	 *
	 */
	@Test(enabled = true)
	public void testTicket39() {
		String dbName = "ticket39_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		Point point = Point.measurement("my_type")
				.field("my_field", "string_value")
				.tag("async", "true")
				.build();
		this.influxDB.write(dbName, "default", ConsistencyLevel.ALL, Lists.newArrayList(point));
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test for ticket #40
	 */
	@Test(enabled = true)
	public void testTicket40() {
		String dbName = "ticket40_" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		this.influxDB.enableBatch(100, 100, TimeUnit.MICROSECONDS);
		for (int i = 0; i < 1000; i++) {
			Point point = Point.measurement("cpu").field("idle", 99).build();
			this.influxDB.write(dbName, "default", ConsistencyLevel.ONE, point);
		}
		this.influxDB.deleteDatabase(dbName);
	}

}
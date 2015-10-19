package org.influxdb;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Test the InfluxDB API.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
@Test(enabled=false)
public class InfluxDBTest {

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
				.withUri("tcp://localhost:4243")
				.withUsername("roott")
				.withPassword("root")
				.build();
		dockerClient = DockerClientBuilder.getInstance(config).build();
		// dockerClient.pullImageCmd("majst01/influxdb-java");

		// ExposedPort tcp8086 = ExposedPort.tcp(8086);
		//
		// Ports portBindings = new Ports();
		// portBindings.bind(tcp8086, Ports.Binding(8086));
		// container = dockerClient.createContainerCmd("influxdb:0.9.0-rc7").exec();
		// dockerClient.startContainerCmd(container.getId()).withPortBindings(portBindings).exec();
		//
		// InspectContainerResponse inspectContainerResponse =
		// dockerClient.inspectContainerCmd(
		// container.getId()).exec();
		//
		// InputStream containerLogsStream = dockerClient
		// .logContainerCmd(container.getId())
		// .withStdErr()
		// .withStdOut()
		// .exec();

		// String ip = inspectContainerResponse.getNetworkSettings().getIpAddress();
		String ip = "127.0.0.1";
		influxDB = InfluxDBFactory.connect("http://" + ip + ":8086", "root", "root");
		boolean influxDBstarted = false;
		do {
			Pong response;
			try {
				response = influxDB.ping();
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
		influxDB.setLogLevel(LogLevel.FULL);
		// String logs = CharStreams.toString(new InputStreamReader(containerLogsStream,
		// Charsets.UTF_8));
		System.out.println("##################################################################################");
		// System.out.println("Container Logs: \n" + logs);
		System.out.println("#  Connected to InfluxDB Version: " + influxDB.version() + " #");
		System.out.println("##################################################################################");
	}

	/**
	 * Ensure all Databases created get dropped afterwards.
	 */
	@AfterClass
	public void tearDown() {
		System.out.println("Kill the Docker container");
		// dockerClient.killContainerCmd(container.getId()).exec();
	}

	/**
	 * Test for a ping.
	 */
	@Test(enabled = false)
	public void testPing() {
		Pong result = influxDB.ping();
		Assert.assertNotNull(result);
		Assert.assertNotEquals(result.getVersion(), "unknown");
	}

	/**
	 * Test that version works.
	 */
	@Test(enabled = false)
	public void testVersion() {
		String version = influxDB.version();
		Assert.assertNotNull(version);
		Assert.assertFalse(version.contains("unknown"));
	}

	/**
	 * Simple Test for a query.
	 */
	@Test(enabled = false)
	public void testQuery() {
		influxDB.query(new Query("CREATE DATABASE mydb2", "mydb"));
		influxDB.query(new Query("DROP DATABASE mydb2", "mydb"));
	}

	/**
	 * Test that describe Databases works.
	 */
	@Test(enabled = false)
	public void testDescribeDatabases() {
		String dbName = "unittest_" + System.currentTimeMillis();
		influxDB.createDatabase(dbName);
		influxDB.describeDatabases();
		List<String> result = influxDB.describeDatabases();
		Assert.assertNotNull(result);
		Assert.assertTrue(result.size() > 0);
		boolean found = false;
		for (String database : result) {
			if (database.equals(dbName)) {
				found = true;
				break;
			}

		}
		Assert.assertTrue(found, "It is expected that describeDataBases contents the newly create database.");
	}

	/**
	 * Test that writing to the new lineprotocol.
	 */
	@Test(enabled = false)
	public void testWrite() {
		String dbName = "write_unittest_" + System.currentTimeMillis();
		influxDB.createDatabase(dbName);

		Point point1 = Point
				.measurement("cpu")
				.tag("async", "true")
				.tag("atag", "test")
				.field("idle", 90L)
				.field("usertime", 9L)
				.field("system", 1L)
				.build();
		Point point2 = Point.measurement("disk")
				.tag("async", "true")
				.tag("atag", "test")
				.field("used", 80L)
				.field("free", 1L).build();
		influxDB.write(dbName, "default", ConsistencyLevel.ONE, Lists.newArrayList(point1, point2));
		
		Query query = new Query("SELECT * FROM cpu GROUP BY *", dbName);
		QueryResult result = influxDB.query(query);
		Assert.assertFalse(result.getResults().get(0).getSeries().get(0).getTags().isEmpty());
		influxDB.deleteDatabase(dbName);
	}
}

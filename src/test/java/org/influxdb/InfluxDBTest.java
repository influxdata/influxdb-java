package org.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.Pong;
import org.influxdb.dto.ScheduledDelete;
import org.influxdb.dto.Serie;
import org.influxdb.dto.User;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.kpelykh.docker.client.DockerClient;
import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.Ports;
import com.kpelykh.docker.client.model.Ports.Port;

/**
 * Test the InfluxDB API.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
@Test
public class InfluxDBTest {

	private InfluxDB influxDB;
	private DockerClient dockerClient;
	private ContainerCreateResponse container;

	/**
	 * Create a influxDB connection before all tests start.
	 * 
	 * @throws DockerException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public void setUp() throws DockerException, InterruptedException {
		this.dockerClient = new DockerClient("http://localhost:4243");
		this.dockerClient.pull("majst01/influxdb-java");

		ContainerConfig containerConfig = new ContainerConfig();
		containerConfig.setImage("majst01/influxdb-java");
		this.container = this.dockerClient.createContainer(containerConfig);
		HostConfig hostconfig = new HostConfig();
		hostconfig.setPortBindings(new Ports());
		hostconfig.getPortBindings().addPort(new Port("tcp", "8086", "0.0.0.0", "8086"));
		this.dockerClient.startContainer(this.container.getId(), hostconfig);

		this.influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
		boolean influxDBstarted = false;
		do {
			Pong response;
			try {
				response = this.influxDB.ping();
				if (response.getStatus().equalsIgnoreCase("ok")) {
					influxDBstarted = true;
				}
			} catch (Exception e) {
				// NOOP intentional
			}
			Thread.sleep(100L);
		} while (!influxDBstarted);
		this.influxDB.setLogLevel(LogLevel.NONE);
	}

	/**
	 * Ensure all Databases created get dropped afterwards.
	 * 
	 * @throws DockerException
	 */
	@AfterClass
	public void tearDown() throws DockerException {
		List<Database> result = this.influxDB.describeDatabases();
		for (Database database : result) {
			this.influxDB.deleteDatabase(database.getName());
		}

		this.dockerClient.kill(this.container.getId());
	}

	/**
	 * Test for a ping.
	 */
	@Test
	public void pingTest() {
		Pong result = this.influxDB.ping();
		Assert.assertNotNull(result);
		Assert.assertEquals(result.getStatus(), "ok");
	}

	/**
	 * Test that describe Databases works.
	 */
	@Test
	public void describeDatabasesTest() {
		String dbName = "unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		List<Database> result = this.influxDB.describeDatabases();
		Assert.assertNotNull(result);
		Assert.assertTrue(result.size() > 0);
		for (Database database : result) {
			System.out.println(database.getName() + " " + database.getReplicationFactor());
		}
	}

	/**
	 * Test that deletion of a Database works.
	 */
	@Test
	public void deleteDatabaseTest() {
		List<Database> result = this.influxDB.describeDatabases();
		int databases = result.size();
		this.influxDB.createDatabase("toDelete", 1);
		result = this.influxDB.describeDatabases();
		Assert.assertEquals(result.size(), databases + 1);
		this.influxDB.deleteDatabase("toDelete");
		result = this.influxDB.describeDatabases();
		Assert.assertEquals(result.size(), databases);
		// Creation of the same database must succeed.
		this.influxDB.createDatabase("toDelete", 1);
		this.influxDB.deleteDatabase("toDelete");

		result = this.influxDB.describeDatabases();
		Assert.assertEquals(result.size(), databases);
	}

	/**
	 * Test that writing of a simple Serie works.
	 */
	@Test
	public void writeTest() {
		String dbName = "write-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);

		Serie serie = new Serie.Builder("testSeries")
				.columns("value1", "value2")
				.values(System.currentTimeMillis(), 5)
				.build();
		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, serie);

		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test for the new Serie.Builder.
	 */
	@Test
	public void writeWithSerieBuilder() {
		String dbName = "writeseriebuilder-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		int outer = 20;
		Stopwatch watch = Stopwatch.createStarted();
		List<Serie> series = Lists.newArrayList();
		for (int i = 0; i < outer; i++) {
			Serie serie = new Serie.Builder("serieFromBuilder")
					.columns("column1", "column2")
					.values(System.currentTimeMillis(), 1)
					.values(System.currentTimeMillis(), 2)
					.values(System.currentTimeMillis(), 3)
					.values(System.currentTimeMillis(), 4)
					.values(System.currentTimeMillis(), 5)
					.values(System.currentTimeMillis(), 6)
					.values(System.currentTimeMillis(), 7)
					.values(System.currentTimeMillis(), 8)
					.values(System.currentTimeMillis(), 9)
					.values(System.currentTimeMillis(), 10)
					.build();
			series.add(serie);
		}
		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, series.toArray(new Serie[0]));

		System.out.println("Inserted " + outer + " Datapoints in " + watch);
		this.influxDB.deleteDatabase(dbName);

		try {
			new Serie.Builder("").columns("column1", "column2").values(System.currentTimeMillis(), 1).build();
			Assert.fail("It is expected that creation of a Serie with a empty Name fails.");
		} catch (Exception e) {
			// Expected.
		}

		try {
			new Serie.Builder("SerieWith wrong value count")
					.columns("column1", "column2")
					.values(System.currentTimeMillis(), 1, 2)
					.build();
			Assert.fail("It is expected that creation of a Serie with more values than columns fails");
		} catch (Exception e) {
			// Expected.
		}

		try {
			new Serie.Builder("SerieWith wrong value count")
					.columns("column1", "column2", "column3")
					.values(System.currentTimeMillis(), 1)
					.build();
			Assert.fail("It is expected that creation of a Serie with more columns than values fails");
		} catch (Exception e) {
			// Expected.
		}

		try {
			new Serie.Builder("SerieWith wrong value count")
					.columns("column1", "column2")
					.values(System.currentTimeMillis(), 1)
					.columns("column1", "column2")
					.build();
			Assert.fail("It is expected that creation of a Serie and calling columns more than once fails.");
		} catch (Exception e) {
			// Expected.
		}
	}

	/**
	 * Test that querying works.
	 */
	@Test
	public void queryTest() {
		String dbName = "query-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);

		Serie serie = new Serie.Builder("testSeries")
				.columns("value1", "value2")
				.values(System.currentTimeMillis(), 5)
				.build();

		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, serie);

		List<Serie> result = this.influxDB.Query(dbName, "select value2 from testSeries", TimeUnit.MILLISECONDS);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);
		// [{"name":"testSeries","columns":["time","sequence_number","value"],"points":[[1398412802823,160001,5]]}]
		Assert.assertEquals((Double) result.get(0).getRows().get(0).get("value2"), 5d, 0d);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that querying works.
	 */
	@Test
	public void complexQueryTest() {
		String dbName = "complexquery-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);

		Serie serie = new Serie.Builder("testSeries")
				.columns("time", "idle", "steal", "system", "customername")
				.values(System.currentTimeMillis(), 5d, 4d, 3d, "aCustomer")
				.values(System.currentTimeMillis(), 15d, 14d, 13d, "aCustomer1")
				.values(System.currentTimeMillis(), 25d, 24d, 23d, "aCustomer2")
				.build();

		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, serie);

		List<Serie> result = this.influxDB.Query(
				dbName,
				"select time, idle, steal, system, customername from testSeries",
				TimeUnit.MILLISECONDS);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);

		Assert.assertNotNull(result.get(0).getRows());
		Assert.assertTrue(result.get(0).getRows().size() == 3);

		Assert.assertEquals((Double) result.get(0).getRows().get(2).get("idle"), 5d, 0d);
		Assert.assertEquals((Double) result.get(0).getRows().get(2).get("system"), 3d, 0d);
		Assert.assertEquals((String) result.get(0).getRows().get(2).get("customername"), "aCustomer");

		Assert.assertEquals((Double) result.get(0).getRows().get(1).get("idle"), 15d, 0d);
		Assert.assertEquals((Double) result.get(0).getRows().get(1).get("system"), 13d, 0d);
		Assert.assertEquals((String) result.get(0).getRows().get(1).get("customername"), "aCustomer1");

		Assert.assertEquals((Double) result.get(0).getRows().get(0).get("idle"), 25d, 0d);
		Assert.assertEquals((Double) result.get(0).getRows().get(0).get("system"), 23d, 0d);
		Assert.assertEquals((String) result.get(0).getRows().get(0).get("customername"), "aCustomer2");

		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that the creation, describe and deletion of a cluster admin works.
	 */
	@Test
	public void testCreateDescribeDeleteClusterAdmin() {
		List<User> admins = this.influxDB.describeClusterAdmins();
		int adminCount = admins.size();
		this.influxDB.createClusterAdmin("aUser", "aPassword");
		admins = this.influxDB.describeClusterAdmins();
		Assert.assertNotNull(admins);
		Assert.assertTrue(admins.size() == adminCount + 1);
		this.influxDB.deleteClusterAdmin("aUser");
		admins = this.influxDB.describeClusterAdmins();
		Assert.assertNotNull(admins);
		Assert.assertTrue(admins.size() == adminCount);
	}

	/**
	 * Test that the update of a cluster admin works.
	 */
	@Test
	public void testUpdateClusterAdmin() {
		this.influxDB.createClusterAdmin("aAdmin", "aPassword");
		this.influxDB.updateClusterAdmin("aAdmin", "aNewPassword");
		this.influxDB.deleteClusterAdmin("aAdmin");
	}

	/**
	 * Test that the creation, describe and deletion of a database user works.
	 */
	@Test
	public void testCreateDescribeDeleteDatabaseUser() {
		String dbName = "createuser-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		List<User> users = this.influxDB.describeDatabaseUsers(dbName);
		int userCount = users.size();
		this.influxDB.createDatabaseUser(dbName, "aUser", "aPassword");
		users = this.influxDB.describeDatabaseUsers(dbName);
		Assert.assertNotNull(users);
		Assert.assertTrue(users.size() == userCount + 1);
		this.influxDB.deleteDatabaseUser(dbName, "aUser");
		users = this.influxDB.describeDatabaseUsers(dbName);
		Assert.assertNotNull(users);
		Assert.assertTrue(users.size() == userCount);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that the password and admin role change of a database user works.
	 */
	@Test
	public void testUpdateAlterDatabaseUser() {
		String dbName = "updateuser-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		this.influxDB.createDatabaseUser(dbName, "aUser", "aPassword");
		this.influxDB.updateDatabaseUser(dbName, "aUser", "aNewPassword");
		this.influxDB.alterDatabasePrivilege(dbName, "aUser", true);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that the password and admin role change and permission change of a database user works.
	 */
	@Test
	public void testUpdateAlterDatabaseUserWithPermissions() {
		String dbName = "updateuserpermission-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		this.influxDB.createDatabaseUser(dbName, "aUser", "aPassword", "^$", dbName);
		this.influxDB.alterDatabasePrivilege(dbName, "aUser", true, "^$", dbName);

		try {
			this.influxDB.createDatabaseUser(dbName, "aUser", "aPassword", "^$");
			Assert.fail("It is expected that createDatabaseUser fails with the wrong amount of permissions.");
		} catch (Exception e) {
			// Expected
		}
		try {
			this.influxDB.alterDatabasePrivilege(dbName, "aUser", true, "^$", dbName, "invalid");
			Assert.fail("It is expected that alterDatabasePrivilege fails with the wrong amount of permissions.");
		} catch (Exception e) {
			// Expected
		}

		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that the password change of a database user works.
	 */
	@Test(enabled = true)
	public void testAuthenticateDatabaseUser() {
		String dbName = "testAuthenticateDatabaseUser-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		this.influxDB.createDatabaseUser(dbName, "aTestUser", "aTestUserPassword");
		this.influxDB.authenticateDatabaseUser(dbName, "aTestUser", "aTestUserPassword");
		this.influxDB.updateDatabaseUser(dbName, "aTestUser", "aNewPassword");
		this.influxDB.authenticateDatabaseUser(dbName, "aTestUser", "aNewPassword");
		try {
			this.influxDB.authenticateDatabaseUser(dbName, "aTestUser", "aWrongPassword");
			Assert.fail("It is expected that authenticateDataBaseUser with a wrong password fails.");
		} catch (Exception e) {
			// Expected
		}
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that adding a continous query works.
	 */
	@Test
	public void testContinuousQueries() {
		String dbName = "continuousquery-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		this.influxDB.Query(dbName, "select * from clicks into events.global;", TimeUnit.MILLISECONDS);

		List<ContinuousQuery> result = this.influxDB.describeContinuousQueries(dbName);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);

		this.influxDB.deleteContinuousQuery(dbName, result.get(0).getId());
		result = this.influxDB.describeContinuousQueries(dbName);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 0);

		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test is disabled because this is not implemented in influxDB.
	 */
	@Test(enabled = false)
	public void testCreateDeleteDescribeScheduledDeletes() {
		String dbName = "scheduleddeletes-unittest-" + System.currentTimeMillis();
		List<ScheduledDelete> deletes = this.influxDB.describeScheduledDeletes(dbName);
		Assert.assertNull(deletes);
		Assert.assertEquals(deletes.size(), 0);
	}

	/**
	 * Test that deletion of points works.
	 */
	@Test
	public void testDeletePoints() {
		String dbName = "deletepoints-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);

		Serie serie = new Serie.Builder("testSeries")
				.columns("value1", "value2")
				.values(System.currentTimeMillis(), 5)
				.build();
		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, serie);

		List<Serie> result = this.influxDB.Query(dbName, "select value1 from testSeries", TimeUnit.MILLISECONDS);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);

		this.influxDB.deletePoints(dbName, "testSeries");
		result = this.influxDB.Query(dbName, "select value1 from testSeries", TimeUnit.MILLISECONDS);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 0);

		this.influxDB.deleteDatabase(dbName);
	}
}

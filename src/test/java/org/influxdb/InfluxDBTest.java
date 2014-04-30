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
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
			Thread.sleep(100L);
		} while (!influxDBstarted);
		this.influxDB.setLogLevel(LogLevel.FULL);
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

		Serie serie = new Serie("testSeries");
		serie.setColumns(new String[] { "value1", "value2" });
		Object[] point = new Object[] { System.currentTimeMillis(), 5 };
		serie.setPoints(new Object[][] { point });
		Serie[] series = new Serie[] { serie };
		this.influxDB.write(dbName, series, TimeUnit.MILLISECONDS);

		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that writing of a many Series works.
	 */
	@Test()
	public void writeManyTest() {
		String dbName = "writemany-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		int outer = 20;
		int inner = 50;
		Stopwatch watch = Stopwatch.createStarted();
		for (int i = 0; i < outer; i++) {
			Serie serie = new Serie("testSeries");
			serie.setColumns(new String[] { "value1", "value2" });
			Object[][] points = new Object[inner][2];
			for (int j = 0; j < inner; j++) {
				Object[] point = new Object[2];
				point[0] = System.currentTimeMillis();
				point[1] = 50f + (Math.random() * 10);
				points[j] = point;
			}
			serie.setPoints(points);
			Serie[] series = new Serie[] { serie };
			this.influxDB.write(dbName, series, TimeUnit.MILLISECONDS);
		}
		System.out.println("Inserted " + (outer * inner) + " Datapoints in " + watch);
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that querying works.
	 */
	@Test
	public void queryTest() {
		String dbName = "query-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);

		Serie serie = new Serie("testSeries");
		serie.setColumns(new String[] { "value1", "value2" });
		Object[] point = new Object[] { System.currentTimeMillis(), 5 };
		serie.setPoints(new Object[][] { point });

		Serie[] series = new Serie[] { serie };
		this.influxDB.write(dbName, series, TimeUnit.MILLISECONDS);

		List<Serie> result = this.influxDB.Query(dbName, "select value2 from testSeries", TimeUnit.MILLISECONDS);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);
		// [{"name":"testSeries","columns":["time","sequence_number","value"],"points":[[1398412802823,160001,5]]}]
		Assert.assertEquals((Double) result.get(0).getPoints()[0][2], 5d, 0d);
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
	// FIXME dont now why this accidently does not work anymore.
	@Test(enabled = false)
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

		Serie serie = new Serie("testSeries");
		serie.setColumns(new String[] { "value1", "value2" });
		Object[] point = new Object[] { System.currentTimeMillis(), 5 };
		serie.setPoints(new Object[][] { point });
		Serie[] series = new Serie[] { serie };
		this.influxDB.write(dbName, series, TimeUnit.MILLISECONDS);

		List<Serie> result = this.influxDB.Query(dbName, "select value1 from testSeries", TimeUnit.MILLISECONDS);
		Assert.assertNotNull(series);
		Assert.assertEquals(result.size(), 1);

		this.influxDB.deletePoints(dbName, "testSeries");
		result = this.influxDB.Query(dbName, "select value1 from testSeries", TimeUnit.MILLISECONDS);
		Assert.assertNotNull(series);
		Assert.assertEquals(result.size(), 0);

		this.influxDB.deleteDatabase(dbName);
	}
}

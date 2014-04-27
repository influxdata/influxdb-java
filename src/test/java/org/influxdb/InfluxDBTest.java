package org.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Serie;
import org.influxdb.dto.User;
import org.influxdb.InfluxDBFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;

/**
 * Test the InfluxDB API.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
@Test
public class InfluxDBTest {

	private InfluxDB influxDB;

	@BeforeClass
	public void setUp() {
		this.influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
		this.influxDB.setLogLevel(LogLevel.FULL);
	}

	/**
	 * Ensure all Databases created get dropped afterwards.
	 * 
	 */
	@AfterClass
	public void tearDown() {
		List<Database> result = this.influxDB.describeDatabases();
		for (Database database : result) {
			this.influxDB.deleteDatabase(database.getName());
		}
	}

	@Test
	public void pingTest() {
		Pong result = this.influxDB.ping();
		Assert.assertNotNull(result);
		Assert.assertEquals(result.getStatus(), "ok");
	}

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

	@Test
	public void deleteDatabasesTest() {
		this.influxDB.createDatabase("toDelete", 1);
		this.influxDB.deleteDatabase("toDelete");
		// Creation of the same database must succeed.
		this.influxDB.createDatabase("toDelete", 1);
		this.influxDB.deleteDatabase("toDelete");

		List<Database> result = this.influxDB.describeDatabases();
		for (Database database : result) {
			System.out.println(database.getName() + " " + database.getReplicationFactor());
		}
	}

	@Test
	public void writeTest() {
		String dbName = "write-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);

		Serie serie = new Serie();
		serie.setName("testSeries");
		serie.setColumns(new String[] { "value1", "value2" });
		Object[] point = new Object[] { System.currentTimeMillis(), 5 };
		serie.setPoints(new Object[][] { point });
		Serie[] series = new Serie[] { serie };
		this.influxDB.write(dbName, series, TimeUnit.MILLISECONDS);

		this.influxDB.deleteDatabase(dbName);
	}

	@Test()
	public void writeManyTest() {
		String dbName = "writemany-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		int outer = 20;
		int inner = 50;
		Stopwatch watch = new Stopwatch().start();
		for (int i = 0; i < outer; i++) {
			Serie serie = new Serie();
			serie.setName("testSeries");
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

	@Test
	public void queryTest() {
		String dbName = "query-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);

		Serie serie = new Serie();
		serie.setName("testSeries");
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

	@Test
	public void testUpdateClusterAdmin() {
		this.influxDB.createClusterAdmin("aAdmin", "aPassword");
		this.influxDB.updateClusterAdmin("aAdmin", "aNewPassword");
		this.influxDB.deleteClusterAdmin("aAdmin");
	}

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

	@Test
	public void testUpdateAlterDatabaseUser() {
		String dbName = "updateuser-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		this.influxDB.createDatabaseUser(dbName, "aUser", "aPassword");
		this.influxDB.updateDatabaseUser(dbName, "aUser", "aNewPassword");
		this.influxDB.alterDatabasePrivilege(dbName, "aUser", true);
		this.influxDB.deleteDatabase(dbName);
	}

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

	@Test
	public void testAuthenticateDatabaseUser() {
		String dbName = "authenticateuser-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		this.influxDB.createDatabaseUser(dbName, "aUser", "aPassword");
		this.influxDB.updateDatabaseUser(dbName, "aUser", "aNewPassword");
		this.influxDB.authenticateDatabaseUser(dbName, "aUser", "aNewPassword");
		try {
			this.influxDB.authenticateDatabaseUser(dbName, "aUser", "aWrongPassword");
			Assert.fail("It is expected that authenticateDataBaseUser with a wrong password fails.");
		} catch (Exception e) {
			// Expected
		}
		this.influxDB.deleteDatabase(dbName);
	}

	@Test
	public void testContinuousQueries() {
		String dbName = "continuousquery-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName, 1);
		this.influxDB.Query(dbName, "select * from clicks into events.global;", TimeUnit.MILLISECONDS);

		List<ContinuousQuery> result = this.influxDB.getContinuousQueries(dbName);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);

		this.influxDB.deleteContinuousQuery(dbName, result.get(0).getId());
		result = this.influxDB.getContinuousQueries(dbName);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 0);

		this.influxDB.deleteDatabase(dbName);
	}
}

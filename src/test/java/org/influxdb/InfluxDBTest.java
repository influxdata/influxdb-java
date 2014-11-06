package org.influxdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.DatabaseConfiguration;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Serie;
import org.influxdb.dto.Server;
import org.influxdb.dto.Shard;
import org.influxdb.dto.Shard.Member;
import org.influxdb.dto.ShardSpace;
import org.influxdb.dto.Shards;
import org.influxdb.dto.User;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DockerClientImpl;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;

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
		this.dockerClient = new DockerClientImpl("http://localhost:4243");
		this.dockerClient.pullImageCmd("majst01/influxdb-java");

		ExposedPort tcp8086 = ExposedPort.tcp(8086);

		Ports portBindings = new Ports();
		portBindings.bind(tcp8086, Ports.Binding(8086));
		this.container = this.dockerClient.createContainerCmd("majst01/influxdb-java").exec();
		this.dockerClient.startContainerCmd(this.container.getId()).withPortBindings(portBindings).exec();

		InspectContainerResponse inspectContainerResponse = this.dockerClient.inspectContainerCmd(
				this.container.getId()).exec();

		InputStream containerLogsStream = this.dockerClient
				.logContainerCmd(this.container.getId())
				.withStdErr()
				.withStdOut()
				.exec();

		String ip = inspectContainerResponse.getNetworkSettings().getIpAddress();
		this.influxDB = InfluxDBFactory.connect("http://" + ip + ":8086", "root", "root");
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
		this.influxDB.setLogLevel(LogLevel.FULL);
		String logs = CharStreams.toString(new InputStreamReader(containerLogsStream, Charsets.UTF_8));
		System.out.println("##################################################################################");
		System.out.println("CContainer Logs: \n" + logs);
		System.out.println("#  Connected to InfluxDB Version: " + this.influxDB.version() + " #");
		System.out.println("##################################################################################");
	}

	/**
	 * Ensure all Databases created get dropped afterwards.
	 */
	@AfterClass
	public void tearDown() {
		System.out.println("Kill the Docker container");
		this.dockerClient.killContainerCmd(this.container.getId()).exec();
	}

	/**
	 * Test for a ping.
	 */
	@Test
	public void testPing() {
		Pong result = this.influxDB.ping();
		Assert.assertNotNull(result);
		Assert.assertEquals(result.getStatus(), "ok");
	}

	/**
	 * Test that describe Databases works.
	 */
	@Test
	public void testDescribeDatabases() {
		String dbName = "unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		List<Database> result = this.influxDB.describeDatabases();
		Assert.assertNotNull(result);
		Assert.assertTrue(result.size() > 0);
		boolean found = false;
		for (Database database : result) {
			if (database.getName().equals(dbName)) {
				found = true;
				break;
			}
		}
		Assert.assertTrue(found, "It is expected that describeDataBases contents the newly create database.");
	}

	/**
	 * Test that deletion of a Database works.
	 */
	@Test
	public void testDeleteDatabase() {
		List<Database> result = this.influxDB.describeDatabases();
		int databases = result.size();
		this.influxDB.createDatabase("toDelete");
		result = this.influxDB.describeDatabases();
		Assert.assertEquals(result.size(), databases + 1);
		this.influxDB.deleteDatabase("toDelete");
		result = this.influxDB.describeDatabases();
		Assert.assertEquals(result.size(), databases);
		// Creation of the same database must succeed.
		this.influxDB.createDatabase("toDelete");
		this.influxDB.deleteDatabase("toDelete");

		result = this.influxDB.describeDatabases();
		Assert.assertEquals(result.size(), databases);
	}

	/**
	 * Test that writing of a simple Serie works.
	 */
	@Test
	public void testWrite() {
		String dbName = "write-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		Serie serie = new Serie.Builder("testSeries")
				.columns("value1", "value2")
				.values(System.currentTimeMillis(), 5)
				.build();
		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, serie);

		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that writing via UDP works.
	 */
	@Test
	public void testWriteUDP() {
		String dbName = "write-udp-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		Serie serie = new Serie.Builder("testSeries")
				.columns("value1", "value2")
				.values(System.currentTimeMillis(), 5)
				.build();
		this.influxDB.writeUdp(8088, serie);
		// FIXME we need to check the existence of the data written.
		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test how writing to a nonexisting Database behaves.
	 */
	// FIXME this test should be enabled.
	@Test(enabled = false)
	public void testWriteToNonExistingDatabase() {
		Serie serie = new Serie.Builder("testSeries")
				.columns("value1", "value2")
				.values(System.currentTimeMillis(), 5)
				.build();
		this.influxDB.write("NonExisting", TimeUnit.MILLISECONDS, serie);
		List<Serie> series = this.influxDB.query("NonExisting", "select * from value1", TimeUnit.MILLISECONDS);
		Assert.assertNotNull(series);
		Assert.assertEquals(series.size(), 1);
	}

	/**
	 * Test for the new Serie.Builder.
	 */
	@Test
	public void testWriteWithSerieBuilder() {
		String dbName = "writeseriebuilder-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);
		int outer = 20;
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
	public void testQuery() {
		String dbName = "query-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		Serie serie = new Serie.Builder("testSeries")
				.columns("value1", "value2")
				.values(System.currentTimeMillis(), 5)
				.build();

		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, serie);

		List<Serie> result = this.influxDB.query(dbName, "select value2 from testSeries", TimeUnit.MILLISECONDS);
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
	public void testComplexQuery() {
		String dbName = "complexquery-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		Serie serie = new Serie.Builder("testSeries")
				.columns("time", "idle", "steal", "system", "customername")
				.values(System.currentTimeMillis(), 5d, 4d, 3d, "aCustomer")
				.values(System.currentTimeMillis(), 15d, 14d, 13d, "aCustomer1")
				.values(System.currentTimeMillis(), 25d, 24d, 23d, "aCustomer2")
				.build();

		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, serie);

		List<Serie> result = this.influxDB.query(
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
		this.influxDB.createDatabase(dbName);
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
		this.influxDB.createDatabase(dbName);
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
		this.influxDB.createDatabase(dbName);
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
		this.influxDB.createDatabase(dbName);
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
		this.influxDB.createDatabase(dbName);
		this.influxDB.query(dbName, "select * from clicks into events.global;", TimeUnit.MILLISECONDS);

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
	 * Test that deletion of series works.
	 */
	@Test
	public void testDeleteSeries() {
		String dbName = "deleteseries-unittest-" + System.currentTimeMillis();
		this.influxDB.createDatabase(dbName);

		Serie serie = new Serie.Builder("testSeries")
				.columns("value1", "value2")
				.values(System.currentTimeMillis(), 5)
				.build();
		this.influxDB.write(dbName, TimeUnit.MILLISECONDS, serie);

		List<Serie> result = this.influxDB.query(dbName, "select value1 from testSeries", TimeUnit.MILLISECONDS);
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);

		result = this.influxDB.query(dbName, "list series", TimeUnit.MILLISECONDS);
		Assert.assertTrue(result.get(0).getRows().get(0).containsKey("name"));
		Assert.assertEquals(result.get(0).getRows().get(0).get("name"), "testSeries");

		this.influxDB.deleteSeries(dbName, "testSeries");
		result = this.influxDB.query(dbName, "list series", TimeUnit.MILLISECONDS);
		Assert.assertEquals(result.get(0).getRows().size(), 0);

		this.influxDB.deleteDatabase(dbName);
	}

	/**
	 * Test that version works.
	 */
	@Test
	public void testVersion() {
		String version = this.influxDB.version();
		Assert.assertNotNull(version);
	}

	/**
	 * Test that compaction works.
	 */
	@Test
	public void testForceRaftCompaction() {
		this.influxDB.forceRaftCompaction();
	}

	/**
	 * Test that list interfaces works.
	 */
	@Test
	public void testInterfaces() {
		List<String> interfaces = this.influxDB.interfaces();
		Assert.assertNotNull(interfaces);
		Assert.assertTrue(interfaces.size() > 0);
		Assert.assertEquals(interfaces.get(0), "default");
	}

	/**
	 * Test that sync works.
	 */
	@Test
	public void testSync() {
		Boolean executed = this.influxDB.sync();
		Assert.assertTrue(executed);
	}

	/**
	 * Test that list servers works.
	 */
	@Test
	public void testListServers() {
		List<Server> servers = this.influxDB.listServers();
		Assert.assertNotNull(servers);
		Assert.assertTrue(servers.size() > 0);
		Assert.assertEquals(servers.get(0).getId(), 1);
	}

	/**
	 * Test that remove servers works.
	 */
	@Test
	public void testRemoveServers() {
		this.influxDB.removeServers(2);
	}

	/**
	 * Test that describe, create and drop of shards works.
	 * 
	 * @deprecated this functionality is gone with 0.8.0, remove this test and related
	 *             implementations.
	 */
	@Deprecated
	@Test(enabled = false)
	public void testDescribeCreateDropShard() {
		Shards existingShards = this.influxDB.getShards();
		Assert.assertNotNull(existingShards);
		Assert.assertNotNull(existingShards.getLongTerm());
		Assert.assertNotNull(existingShards.getShortTerm());
		int existingLongTermShards = existingShards.getLongTerm().size();
		int existingShortTermShards = existingShards.getShortTerm().size();

		Shard shard = new Shard();
		long now = System.currentTimeMillis() / 1000L;
		shard.setStartTime(now);
		shard.setEndTime(now + 10000L);
		shard.setLongTerm(false);
		Member shards = new Member();
		shards.setServerIds(ImmutableList.of(1));
		shard.setShards(ImmutableList.of(shards));
		this.influxDB.createShard(shard);

		Shard shard2 = new Shard();
		shard.setStartTime(now);
		shard.setEndTime(now + 10000L);
		shard.setLongTerm(false);
		Member shards2 = new Member();
		shards2.setServerIds(ImmutableList.of(1));
		shard2.setShards(ImmutableList.of(shards2));
		this.influxDB.createShard(shard2);

		Shards createdShards = this.influxDB.getShards();

		Assert.assertNotNull(createdShards);
		Assert.assertNotNull(createdShards.getLongTerm());
		Assert.assertNotNull(createdShards.getShortTerm());
		Assert.assertEquals(createdShards.getShortTerm().size(), existingShortTermShards + 2);
		Assert.assertEquals(createdShards.getLongTerm().size(), existingLongTermShards);

		shard.setId(2);
		shard2.setId(3);
		this.influxDB.dropShard(shard);
		existingShards = this.influxDB.getShards();
		Assert.assertEquals(existingShards.getShortTerm().size(), existingShortTermShards + 1);
		this.influxDB.dropShard(shard2);
		existingShards = this.influxDB.getShards();
		Assert.assertEquals(existingShards.getShortTerm().size(), existingShortTermShards);
	}

	/**
	 * Test that describe, create and drop of shardspaces works.
	 */
	@Test
	public void testDescribeCreateDropShardSpaces() {
		String dbName = "describecreatedropshard-unittest-" + System.currentTimeMillis();
		DatabaseConfiguration config = new DatabaseConfiguration(dbName);
		ShardSpace shard = new ShardSpace("default", "inf", "7d", "/.*/", 1, 1);
		config.addSpace(shard);
		List<ShardSpace> existingShards = this.influxDB.getShardSpaces();
		this.influxDB.createDatabase(config);
		List<ShardSpace> existingShardsAfter = this.influxDB.getShardSpaces();
		Assert.assertEquals(existingShardsAfter.size() - existingShards.size(), 1);
		Assert.assertEquals(existingShardsAfter.get(0).getDatabase(), dbName);
		Assert.assertEquals(existingShardsAfter.get(0).getName(), "default");
		Assert.assertEquals(existingShardsAfter.get(0).getRetentionPolicy(), "inf");
		Assert.assertEquals(existingShardsAfter.get(0).getShardDuration(), "7d");
		Assert.assertEquals(existingShardsAfter.get(0).getReplicationFactor(), 1);
		Assert.assertEquals(existingShardsAfter.get(0).getSplit(), 1);

		this.influxDB.dropShardSpace(dbName, "default");
		existingShardsAfter = this.influxDB.getShardSpaces();
		Assert.assertEquals(existingShardsAfter.size() - existingShards.size(), 0);

		ShardSpace fourDayShard = new ShardSpace("4day", "inf", "4d", "/.*/", 1, 1);
		this.influxDB.createShardSpace(dbName, fourDayShard);

		existingShardsAfter = this.influxDB.getShardSpaces();
		Assert.assertEquals(existingShardsAfter.size() - existingShards.size(), 1);
		Assert.assertEquals(existingShardsAfter.get(0).getDatabase(), dbName);
		Assert.assertEquals(existingShardsAfter.get(0).getName(), "4day");
		Assert.assertEquals(existingShardsAfter.get(0).getRetentionPolicy(), "inf");
		Assert.assertEquals(existingShardsAfter.get(0).getShardDuration(), "4d");
		Assert.assertEquals(existingShardsAfter.get(0).getReplicationFactor(), 1);
		Assert.assertEquals(existingShardsAfter.get(0).getSplit(), 1);

		this.influxDB.dropShardSpace(dbName, "4day");
		existingShardsAfter = this.influxDB.getShardSpaces();
		Assert.assertEquals(existingShardsAfter.size() - existingShards.size(), 0);
	}
}
package org.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.DatabaseConfiguration;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Serie;
import org.influxdb.dto.Server;
import org.influxdb.dto.Shard;
import org.influxdb.dto.ShardSpace;
import org.influxdb.dto.Shards;
import org.influxdb.dto.User;

/**
 * Interface with all available methods to access a InfluxDB database.
 * 
 * A full list of currently available interfaces is implemented in:
 * 
 * <a
 * href="https://github.com/influxdb/influxdb/blob/master/src/api/http/api.go">https://github.com/
 * influxdb/influxdb/blob/master/src/api/http/api.go</a>
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public interface InfluxDB {

	/** Controls the level of logging of the REST layer. */
	public enum LogLevel {
		/** No logging. */
		NONE,
		/** Log only the request method and URL and the response status code and execution time. */
		BASIC,
		/** Log the basic information along with request and response headers. */
		HEADERS,
		/**
		 * Log the headers, body, and metadata for both requests and responses.
		 * <p>
		 * Note: This requires that the entire request and response body be buffered in memory!
		 */
		FULL;
	}

	/**
	 * Set the loglevel which is used for REST related actions.
	 * 
	 * @param logLevel
	 *            the loglevel to set.
	 * @return the InfluxDB instance to be able to use it in a fluent manner.
	 */
	public InfluxDB setLogLevel(final LogLevel logLevel);

	/**
	 * Ping this influxDB-
	 * 
	 * @return the response of the ping execution.
	 */
	public Pong ping();

	/**
	 * Return the version of the connected influxDB Server.
	 * 
	 * @return the version String, otherwise unknown.
	 */
	public String version();

	/**
	 * Write a Series to the given database.
	 * 
	 * @param database
	 *            the name of the database to write to.
	 * @param precision
	 *            the precision used for the values.
	 * @param series
	 *            a Array of {@link Serie}s to write.
	 */
	public void write(final String database, final TimeUnit precision, final Serie... series);

	/**
	 * Write a Series to the given database.
	 *
	 * @param port
	 *            the port where to reach the influxdb udp service. The database is configured per
	 *            port in the influxdb configuration.
	 * @param precision
	 *            the precision used for the values.
	 * @param series
	 *            a Array of {@link Serie}s to write.
	 */
	public void writeUdp(final int port, final TimeUnit precision, final Serie... series);

	/**
	 * Execute a query agains a database.
	 * 
	 * @param database
	 *            the name of the database.
	 * @param query
	 *            the query to execute, for language specification please see <a
	 *            href="http://influxdb.org/docs/query_language"
	 *            >http://influxdb.org/docs/query_language</a>
	 * @param precision
	 *            the precision used for the values.
	 * @return a List of Series which matched the query.
	 */
	public List<Serie> query(final String database, final String query, final TimeUnit precision);

	/**
	 * Create a new Database.
	 * 
	 * @param name
	 *            the name of the new database.
	 */
	public void createDatabase(final String name);

	/**
	 * Create a new Database from a {@link DatabaseConfiguration}. This is the way to create a db
	 * with shards specified.
	 * 
	 * @param config
	 *            the configuration for the database to create..
	 */
	public void createDatabase(final DatabaseConfiguration config);

	/**
	 * Delete a database.
	 * 
	 * @param name
	 *            the name of the database to delete.
	 */
	public void deleteDatabase(final String name);

	/**
	 * Describe all available databases.
	 * 
	 * @return a List of all Databases.
	 */
	public List<Database> describeDatabases();

	/**
	 * Create a new cluster admin.
	 * 
	 * @param name
	 *            the name of the new admin.
	 * @param password
	 *            the password for the new admin.
	 */
	public void createClusterAdmin(final String name, final String password);

	/**
	 * Delete a cluster admin.
	 * 
	 * @param name
	 *            the name of the admin to delete.
	 */
	public void deleteClusterAdmin(final String name);

	/**
	 * Describe all cluster admins.
	 * 
	 * @return a list of all admins.
	 */
	public List<User> describeClusterAdmins();

	/**
	 * Update the password of the given admin.
	 * 
	 * @param name
	 *            the name of the admin for which the password should be updated.
	 * @param password
	 *            the new password for the given admin.
	 */
	public void updateClusterAdmin(final String name, final String password);

	/**
	 * Create a new regular database user. Without any given permissions the new user is allowed to
	 * read and write to the database. The permission must be specified in regex which will match
	 * for the series. You have to specify either no permissions or both (readFrom and writeTo)
	 * permissions.
	 * 
	 * @param database
	 *            the name of the database where this user is allowed.
	 * @param name
	 *            the name of the new database user.
	 * @param password
	 *            the password for this user.
	 * @param permissions
	 *            a array of readFrom and writeTo permissions (in this order) and given in regex
	 *            form.
	 */
	public void createDatabaseUser(final String database, final String name, final String password,
			final String... permissions);

	/**
	 * Delete a database user.
	 * 
	 * @param database
	 *            the name of the database the given user should be removed from.
	 * @param name
	 *            the name of the user to remove.
	 */
	public void deleteDatabaseUser(final String database, final String name);

	/**
	 * Describe all database users allowed to acces the given database.
	 * 
	 * @param database
	 *            the name of the database for which all users should be described.
	 * @return a list of all users.
	 */
	public List<User> describeDatabaseUsers(final String database);

	/**
	 * Update the password and/or the permissions of a database user.
	 * 
	 * @param database
	 *            the name of the database where this user is allowed.
	 * @param name
	 *            the name of the existing database user.
	 * @param password
	 *            the password for this user.
	 * @param permissions
	 *            a array of readFrom and writeTo permissions (in this order) and given in regex
	 *            form.
	 */
	public void updateDatabaseUser(final String database, final String name, final String password,
			final String... permissions);

	/**
	 * Alter the admin privilege of a given database user.
	 * 
	 * @param database
	 *            the name of the database where this user is allowed.
	 * @param name
	 *            the name of the existing database user.
	 * @param isAdmin
	 *            if set to true this user is a database admin, otherwise it isnt.
	 * @param permissions
	 *            a array of readFrom and writeTo permissions (in this order) and given in regex
	 *            form.
	 */
	public void alterDatabasePrivilege(final String database, final String name, final boolean isAdmin,
			final String... permissions);

	/**
	 * Authenticate with the given credentials against the database.
	 * 
	 * @param database
	 *            the name of the database where this user is allowed.
	 * @param username
	 *            the name of the existing database user.
	 * @param password
	 *            the password for this user.
	 */
	public void authenticateDatabaseUser(final String database, final String username, final String password);

	/**
	 * Describe all contious queries in a database.
	 * 
	 * @param database
	 *            the name of the database for which all continous queries should be described.
	 * @return a list of all contious queries.
	 */
	public List<ContinuousQuery> describeContinuousQueries(final String database);

	/**
	 * Delete a continous query.
	 * 
	 * @param database
	 *            the name of the database for which this query should be deleted.
	 * @param id
	 *            the id of the query.
	 */
	public void deleteContinuousQuery(final String database, final int id);

	/**
	 * Delete a serie.
	 * 
	 * @param database
	 *            the database in which the given serie should be deleted.
	 * @param serieName
	 *            the name of the serie.
	 */
	public void deleteSeries(final String database, final String serieName);

	/**
	 * Force Database compaction.
	 * 
	 */
	public void forceRaftCompaction();

	/**
	 * List all interfaces influxDB is listening.
	 * 
	 * @return a List of interface names.
	 */
	public List<String> interfaces();

	/**
	 * Sync the database to the filesystem.
	 * 
	 * @return true|false if successful.
	 */
	public Boolean sync();

	/**
	 * List all servers which are member of the cluster.
	 * 
	 * @return a List of all influxdb servers.
	 */
	public List<Server> listServers();

	/**
	 * Remove the given Server from the cluster.
	 * 
	 * @param id
	 *            the id of the server to remove.
	 */
	public void removeServers(final int id);

	/**
	 * Create a new Shard.
	 * 
	 * @param shard
	 *            the new shard to create.
	 * @deprecated this functionality is gone with 0.8.0, will be removed in the next version.
	 */
	@Deprecated
	public void createShard(final Shard shard);

	/**
	 * Describe all existing shards.
	 * 
	 * @return a List of all Shards.
	 * @deprecated this functionality is gone with 0.8.0, will be removed in the next version.
	 */
	@Deprecated
	public Shards getShards();

	/**
	 * Drop the given shard.
	 * 
	 * @param shard
	 *            the shard to delete.
	 * @deprecated this functionality is gone with 0.8.0, will be removed in the next version.
	 */
	@Deprecated
	public void dropShard(final Shard shard);

	/**
	 * Describe all existing shardspaces.
	 * 
	 * @return a List of all ShardSpaces.
	 */
	public List<ShardSpace> getShardSpaces();

	/**
	 * Drop a existing ShardSpace from a Database.
	 *
	 * @param database
	 *            the name of the database.
	 * @param name
	 *            the name of the ShardSpace to delete.
	 */
	public void dropShardSpace(final String database, final String name);

	/**
	 * Create a ShardSpace in a Database.
	 *
	 * @param database
	 *            the name of the database.
	 * @param shardSpace
	 *            the shardSpace to create in this database
	 */
	public void createShardSpace(final String database, final ShardSpace shardSpace);
}
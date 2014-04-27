package org.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.Pong;
import org.influxdb.dto.ScheduledDelete;
import org.influxdb.dto.Serie;
import org.influxdb.dto.User;

public interface InfluxDB {

	/** Controls the level of logging. */
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

	public InfluxDB setLogLevel(final LogLevel logLevel);

	public Pong ping();

	public void write(final String database, final Serie[] series, final TimeUnit precision);

	public List<Serie> Query(final String database, final String query, final TimeUnit precision);

	public void createDatabase(final String name, final int replicationFactor);

	public void deleteDatabase(final String name);

	public List<Database> describeDatabases();

	public void createClusterAdmin(final String name, final String password);

	public void deleteClusterAdmin(final String name);

	public List<User> describeClusterAdmins();

	public void updateClusterAdmin(final String name, final String password);

	public void createDatabaseUser(final String database, final String name, final String password,
			final String... permissions);

	public void deleteDatabaseUser(final String database, final String name);

	public List<User> describeDatabaseUsers(final String database);

	public void updateDatabaseUser(final String database, final String name, final String password,
			final String... permissions);

	public void alterDatabasePrivilege(final String database, final String name, final boolean isAdmin,
			final String... permissions);

	public void authenticateDatabaseUser(final String database, final String username, final String password);

	public List<ContinuousQuery> getContinuousQueries(final String database);

	public void deleteContinuousQuery(final String database, final int id);

	public void deletePoints(final String database, final String serieName);

	public void createScheduledDelete(final String database, final ScheduledDelete delete);

	public List<ScheduledDelete> describeScheduledDeletes(final String database);

	public void removeScheduledDelete(final String database, final int id);

}

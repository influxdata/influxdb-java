package org.influxdb.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.Pong;
import org.influxdb.dto.ScheduledDelete;
import org.influxdb.dto.Serie;
import org.influxdb.dto.User;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import retrofit.RestAdapter;

/**
 * Implementation of a InluxDB API.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class InfluxDBImpl implements InfluxDB {
	private final String username;
	private final String password;
	private final RestAdapter restAdapter;
	private final InfluxDBService influxDBService;

	/**
	 * Constructor which should only be used from the InfluxDBFactory.
	 * 
	 * @param url
	 *            the url where the influxdb is accessible.
	 * @param username
	 *            the user to connect.
	 * @param password
	 *            the password for this user.
	 */
	public InfluxDBImpl(final String url, final String username, final String password) {
		super();
		this.username = username;
		this.password = password;

		this.restAdapter = new RestAdapter.Builder()
				.setEndpoint(url)
				.setErrorHandler(new InfluxDBErrorHandler())
				.build();
		this.influxDBService = this.restAdapter.create(InfluxDBService.class);
	}

	@Override
	public InfluxDB setLogLevel(final LogLevel logLevel) {
		switch (logLevel) {
		case NONE:
			this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.NONE);
			break;
		case BASIC:
			this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.BASIC);
			break;
		case HEADERS:
			this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.HEADERS);
			break;
		case FULL:
			this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.FULL);
			break;
		default:
			break;
		}
		return this;
	}

	@Override
	public Pong ping() {
		Stopwatch watch = Stopwatch.createStarted();
		Pong pong = this.influxDBService.ping();
		pong.setResponseTime(watch.elapsed(TimeUnit.MILLISECONDS));
		return pong;
	}

	@Override
	public void write(final String database, final Serie[] series, final TimeUnit precision) {
		this.influxDBService.write(database, series, this.username, this.password, toTimePrecision(precision));
	}

	@Override
	public List<Serie> Query(final String database, final String query, final TimeUnit precision) {
		return this.influxDBService.query(database, query, this.username, this.password, toTimePrecision(precision));
	}

	@Override
	public void createDatabase(final String name, final int replicationFactor) {
		Preconditions.checkArgument(replicationFactor >= 1, "Replicationfactor must be greater or equal to 1.");
		Database db = new Database(name, replicationFactor);
		this.influxDBService.createDatabase(db, this.username, this.password);
	}

	@Override
	public void deleteDatabase(final String name) {
		this.influxDBService.deleteDatabase(name, this.username, this.password);
	}

	@Override
	public List<Database> describeDatabases() {
		return this.influxDBService.describeDatabases(this.username, this.password);
	}

	@Override
	public void createClusterAdmin(final String name, final String adminPassword) {
		User user = new User();
		user.setName(name);
		user.setPassword(adminPassword);
		this.influxDBService.createClusterAdmin(user, this.username, this.password);
	}

	@Override
	public void deleteClusterAdmin(final String name) {
		this.influxDBService.deleteClusterAdmin(name, this.username, this.password);
	}

	@Override
	public List<User> describeClusterAdmins() {
		return this.influxDBService.describeClusterAdmins(this.username, this.password);
	}

	@Override
	public void updateClusterAdmin(final String name, final String adminPassword) {
		User user = new User();
		user.setPassword(adminPassword);
		this.influxDBService.updateClusterAdmin(user, name, this.username, this.password);
	}

	@Override
	public void createDatabaseUser(final String database, final String name, final String userPassword,
			final String... permissions) {
		User user = new User();
		user.setName(name);
		user.setPassword(userPassword);
		user.setPermissions(permissions);
		this.influxDBService.createDatabaseUser(database, user, this.username, this.password);
	}

	@Override
	public void deleteDatabaseUser(final String database, final String name) {
		this.influxDBService.deleteDatabaseUser(database, name, this.username, this.password);
	}

	@Override
	public List<User> describeDatabaseUsers(final String database) {
		return this.influxDBService.describeDatabaseUsers(database, this.username, this.password);
	}

	@Override
	public void updateDatabaseUser(final String database, final String name, final String newPassword,
			final String... permissions) {
		User user = new User();
		user.setPassword(newPassword);
		user.setPermissions(permissions);
		this.influxDBService.updateDatabaseUser(database, user, name, this.username, this.password);
	}

	@Override
	public void alterDatabasePrivilege(final String database, final String name, final boolean isAdmin,
			final String... permissions) {
		User user = new User();
		user.setAdmin(isAdmin);
		user.setPermissions(permissions);
		this.influxDBService.updateDatabaseUser(database, user, name, this.username, this.password);
	}

	@Override
	public void authenticateDatabaseUser(final String database, final String user, final String userPassword) {
		this.influxDBService.authenticateDatabaseUser(database, user, userPassword);
	}

	@Override
	public List<ContinuousQuery> describeContinuousQueries(final String database) {
		return this.influxDBService.getContinuousQueries(database, this.username, this.password);
	}

	@Override
	public void deleteContinuousQuery(final String database, final int id) {
		this.influxDBService.deleteContinuousQuery(database, id, this.username, this.password);
	}

	@Override
	public void deletePoints(final String database, final String serieName) {
		this.influxDBService.deletePoints(database, serieName, this.username, this.password);
	}

	@Override
	public void createScheduledDelete(final String database, final ScheduledDelete delete) {
		throw new IllegalArgumentException(
				"This is not implemented in InfluxDB, please see: https://github.com/influxdb/influxdb/issues/98");
		// this.influxDBService.createScheduledDelete(database, delete, this.username,
		// this.password);
	}

	@Override
	public List<ScheduledDelete> describeScheduledDeletes(final String database) {
		throw new IllegalArgumentException(
				"This is not implemented in InfluxDB, please see: https://github.com/influxdb/influxdb/issues/98");
		// return this.influxDBService.describeScheduledDeletes(database, this.username,
		// this.password);
	}

	@Override
	public void deleteScheduledDelete(final String database, final int id) {
		throw new IllegalArgumentException(
				"This is not implemented in InfluxDB, please see: https://github.com/influxdb/influxdb/issues/98");
		// this.influxDBService.deleteScheduledDelete(database, id, this.username, this.password);
	}

	private static String toTimePrecision(final TimeUnit t) {
		switch (t) {
		case SECONDS:
			return "s";
		case MILLISECONDS:
			return "m";
		case MICROSECONDS:
			return "u";
		default:
			throw new IllegalArgumentException("time precision should be SECONDS or MILLISECONDS or MICROSECONDS");
		}
	}

}

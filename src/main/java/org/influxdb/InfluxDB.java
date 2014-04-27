package org.influxdb;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;

import retrofit.ErrorHandler;
import retrofit.RestAdapter;
import retrofit.RetrofitError;
import retrofit.client.Response;
import retrofit.http.Body;
import retrofit.http.DELETE;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.Path;
import retrofit.http.Query;

/**
 * A Interface to a InfluxDB Database.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class InfluxDB {

	private final String username;
	private final String password;
	private final RestAdapter restAdapter;
	private final InfluxDBService influxDBService;

	public static InfluxDB connect(final String url, final String username, final String password) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "The URL may not be null or empty.");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(username), "The username may not be null or empty.");
		return new InfluxDB(url, username, password);
	}

	private InfluxDB(final String url, final String username, final String password) {
		super();
		this.username = username;
		this.password = password;

		this.restAdapter = new RestAdapter.Builder()
				.setEndpoint(url)
				.setErrorHandler(new InfluxDBErrorHandler())
				.build();
		this.influxDBService = this.restAdapter.create(InfluxDBService.class);
	}

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

	public Pong ping() {
		return this.influxDBService.ping();
	}

	public void write(final String database, final Serie[] series, final TimeUnit precision) {
		this.influxDBService.write(database, series, this.username, this.password, toTimePrecision(precision));
	}

	public List<Serie> Query(final String database, final String query, final TimeUnit precision) {
		return this.influxDBService.query(database, query, this.username, this.password, toTimePrecision(precision));
	}

	public void createDatabase(final String name, final int replicationFactor) {
		Database db = new Database(name, replicationFactor);
		String response = this.influxDBService.createDatabase(db, this.username, this.password);
	}

	public void deleteDatabase(final String name) {
		String response = this.influxDBService.deleteDatabase(name, this.username, this.password);
	}

	public List<Database> describeDatabases() {
		return this.influxDBService.describeDatabases(this.username, this.password);
	}

	public void createClusterAdmin(final String name, final String password) {
		User user = new User();
		user.setName(name);
		user.setPassword(password);
		this.influxDBService.createClusterAdmin(user, this.username, this.password);
	}

	public void deleteClusterAdmin(final String name) {
		this.influxDBService.deleteClusterAdmin(name, this.username, this.password);
	}

	public List<User> describeClusterAdmins() {
		return this.influxDBService.describeClusterAdmins(this.username, this.password);
	}

	public void updateClusterAdmin(final String name, final String password) {
		User user = new User();
		user.setPassword(password);
		this.influxDBService.updateClusterAdmin(user, name, this.username, this.password);
	}

	public void createDatabaseUser(final String database, final String name, final String password,
			final String... permissions) {
		User user = new User();
		user.setName(name);
		user.setPassword(password);
		user.setPermissions(permissions);
		this.influxDBService.createDatabaseUser(database, user, this.username, this.password);
	}

	public void deleteDatabaseUser(final String database, final String name) {
		this.influxDBService.deleteDatabaseUser(database, name, this.username, this.password);
	}

	public List<User> describeDatabaseUsers(final String database) {
		return this.influxDBService.describeDatabaseUsers(database, this.username, this.password);
	}

	public void updateDatabaseUser(final String database, final String name, final String password,
			final String... permissions) {
		User user = new User();
		user.setPassword(password);
		user.setPermissions(permissions);
		this.influxDBService.updateDatabaseUser(database, user, name, this.username, this.password);
	}

	public void alterDatabasePrivilege(final String database, final String name, final boolean isAdmin,
			final String... permissions) {
		User user = new User();
		user.setAdmin(isAdmin);
		user.setPermissions(permissions);
		this.influxDBService.updateDatabaseUser(database, user, name, this.username, this.password);
	}

	public void authenticateDatabaseUser(final String database, final String username, final String password) {
		this.influxDBService.authenticateDatabaseUser(database, username, password);
	}

	public List<ContinuousQuery> getContinuousQueries(final String database) {
		return this.influxDBService.getContinuousQueries(database, this.username, this.password);
	}

	public void deleteContinuousQuery(final String database, final int id) {
		this.influxDBService.deleteContinuousQuery(database, id, this.username, this.password);
	}

	public void deletePoints(final String database, final String serieName) {
		// TODO implement
		throw new IllegalArgumentException();
	}

	public void createScheduledDelete(final String database, final ScheduledDelete delete) {
		// TODO implement
		throw new IllegalArgumentException();
	}

	public List<ScheduledDelete> describeScheduledDeletes(final String database) {
		// TODO implement
		throw new IllegalArgumentException();
	}

	public void removeScheduledDelete(final String database, final int id) {
		// TODO implement
		throw new IllegalArgumentException();
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

	public static class Pong {
		private String status;

		public String getStatus() {
			return this.status;
		}

		public void setStatus(final String status) {
			this.status = status;
		}
	}

	public static class ContinuousQuery {
		private int id;
		private String query;

		public int getId() {
			return this.id;
		}

		public void setId(final int id) {
			this.id = id;
		}

		public String getQuery() {
			return this.query;
		}

		public void setQuery(final String query) {
			this.query = query;
		}
	}

	public static class ScheduledDelete extends ContinuousQuery {
	}

	public static class Database {
		private final String name;
		private final int replicationFactor;

		public Database(final String name, final int replicationFactor) {
			super();
			this.name = name;
			this.replicationFactor = replicationFactor;
		}

		public String getName() {
			return this.name;
		}

		public int getReplicationFactor() {
			return this.replicationFactor;
		}

	}

	public static class User {
		private String name;
		private String password;
		private boolean admin;
		private String readFrom;
		private String writeTo;

		public String getName() {
			return this.name;
		}

		public void setName(final String name) {
			this.name = name;
		}

		public String getPassword() {
			return this.password;
		}

		public void setPassword(final String password) {
			this.password = password;
		}

		public boolean isAdmin() {
			return this.admin;
		}

		public void setAdmin(final boolean admin) {
			this.admin = admin;
		}

		public String getReadFrom() {
			return this.readFrom;
		}

		public void setReadFrom(final String readFrom) {
			this.readFrom = readFrom;
		}

		public String getWriteTo() {
			return this.writeTo;
		}

		public void setWriteTo(final String writeTo) {
			this.writeTo = writeTo;
		}

		public void setPermissions(final String... permissions) {
			if (null != permissions) {
				switch (permissions.length) {
				case 0:
					break;
				case 2:
					this.setReadFrom(permissions[0]);
					this.setWriteTo(permissions[1]);
					break;
				default:
					throw new IllegalArgumentException("You have to specify readFrom and writeTo permissions.");
				}
			}
		}
	}

	public static class Serie {
		private String name;
		private String[] columns;
		private Object[][] points;

		public String getName() {
			return this.name;
		}

		public void setName(final String name) {
			this.name = name;
		}

		public String[] getColumns() {
			return this.columns;
		}

		public void setColumns(final String[] columns) {
			this.columns = columns;
		}

		public Object[][] getPoints() {
			return this.points;
		}

		public void setPoints(final Object[][] points) {
			this.points = points;
		}

	}

	static class InfluxDBErrorHandler implements ErrorHandler {
		@Override
		public Throwable handleError(final RetrofitError cause) {
			Response r = cause.getResponse();
			if (r != null && r.getStatus() >= 400) {
				try {
					return new RuntimeException(CharStreams.toString(new InputStreamReader(
							r.getBody().in(),
							Charsets.UTF_8)));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return cause;
		}
	}

	interface InfluxDBService {

		@GET("/ping")
		public Pong ping();

		@POST("/db")
		public String createDatabase(@Body Database database, @Query("u") String username, @Query("p") String password);

		@DELETE("/db/{name}")
		public String deleteDatabase(@Path("name") String name, @Query("u") String username, @Query("p") String password);

		@GET("/db")
		public List<Database> describeDatabases(@Query("u") String username, @Query("p") String password);

		@POST("/db/{name}/series")
		public String write(@Path("name") String name, @Body Serie[] series, @Query("u") String username,
				@Query("p") String password, @Query("time_precision") String timePrecision);

		@GET("/db/{name}/series")
		public List<Serie> query(@Path("name") String name, @Query("q") String query, @Query("u") String username,
				@Query("p") String password, @Query("time_precision") String timePrecision);

		@POST("/cluster_admins")
		public String createClusterAdmin(@Body User user, @Query("u") String username, @Query("p") String password);

		@DELETE("/cluster_admins/{name}")
		public String deleteClusterAdmin(@Path("name") String name, @Query("u") String username,
				@Query("p") String password);

		@GET("/cluster_admins")
		public List<User> describeClusterAdmins(@Query("u") String username, @Query("p") String password);

		@POST("/cluster_admins/{name}")
		public String updateClusterAdmin(@Body User user, @Path("name") String name, @Query("u") String username,
				@Query("p") String password);

		@POST("/db/{database}/users")
		public String createDatabaseUser(@Path("database") String database, @Body User user,
				@Query("u") String username, @Query("p") String password);

		@DELETE("/db/{database}/users/{name}")
		public String deleteDatabaseUser(@Path("database") String database, @Path("name") String name,
				@Query("u") String username, @Query("p") String password);

		@GET("/db/{database}/users")
		public List<User> describeDatabaseUsers(@Path("database") String database, @Query("u") String username,
				@Query("p") String password);

		@POST("/db/{database}/users/{name}")
		public String updateDatabaseUser(@Path("database") String database, @Body User user, @Path("name") String name,
				@Query("u") String username, @Query("p") String password);

		@GET("/db/{database}/authenticate")
		public String authenticateDatabaseUser(@Path("database") final String database, @Query("u") String username,
				@Query("p") String password);

		@GET("/db/{database}/continuous_queries")
		public List<ContinuousQuery> getContinuousQueries(@Path("database") String database,
				@Query("u") String username, @Query("p") String password);

		@DELETE("/db/{database}/continuous_queries/{id}")
		public String deleteContinuousQuery(@Path("database") String database, @Path("id") int id,
				@Query("u") String username, @Query("p") String password);

	}

}

package org.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

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
	int CONNECT_TIMEOUT_SECONDS_DEFAULT = 15;
	int READ_TIMEOUT_SECONDS_DEFAULT = 20;
	int WRITE_TIMEOUT_SECONDS_DEFAULT = 20;

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
	 * ConsistencyLevel for write Operations.
	 */
	public enum ConsistencyLevel {
		/** Write succeeds only if write reached all cluster members. */
		ALL("all"),
		/** Write succeeds if write reached any cluster members. */
		ANY("any"),
		/** Write succeeds if write reached at least one cluster members. */
		ONE("one"),
		/** Write succeeds only if write reached a quorum of cluster members. */
		QUORUM("quorum");
		private final String value;

		private ConsistencyLevel(final String value) {
			this.value = value;
		}

		/**
		 * Get the String value of the ConsistencyLevel.
		 *
		 * @return the lowercase String.
		 */
		public String value() {
			return this.value;
		}
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
	 * Enable Batching of single Point writes to speed up writes significant. If either actions or
	 * flushDurations is reached first, a batchwrite is issued.
	 *
	 * @param actions
	 *            the number of actions to collect
	 * @param flushDuration
	 *            the time to wait at most.
	 * @param flushDurationTimeUnit
	 * @return the InfluxDB instance to be able to use it in a fluent manner.
	 */
	public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit);

	/**
	 * Disable Batching.
	 */
	public void disableBatch();

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
	 * Write a single Point to the database.
	 * 
	 * @param database
	 *            the database to write to.
	 * @param retentionPolicy
	 *            the retentionPolicy to use.
	 * @param point
	 *            The point to write
	 */
	public void write(final String database, final String retentionPolicy, final Point point);

	/**
	 * Write a set of Points to the influxdb database with the new (>= 0.9.0rc32) lineprotocol.
	 * 
	 * {@linkplain "https://github.com/influxdb/influxdb/pull/2696"}
	 *
	 * @param batchPoints
	 */
	public void write(final BatchPoints batchPoints);

	/**
	 * Execute a query agains a database.
	 * 
	 * @param query
	 *            the query to execute.
	 * @return a List of Series which matched the query.
	 */
	public QueryResult query(final Query query);

	/**
	 * Execute a query agains a database.
	 * 
	 * @param query
	 *            the query to execute.
	 * @param timeUnit the time unit of the results. 
	 * @return a List of Series which matched the query.
	 */
	public QueryResult query(final Query query, TimeUnit timeUnit);

	/**
	 * Create a new Database.
	 *
	 * @param name
	 *            the name of the new database.
	 */
	public void createDatabase(final String name);

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
	 * @return a List of all Database names.
	 */
	public List<String> describeDatabases();

	/**
	 * Sets the default connect timeout for new connections. A value of 0 means no timeout. If not
	 * set explicitly, a default timeout of CONNECT_TIMEOUT_SECONDS_DEFAULT seconds will be used.
	 *
	 * @param connectTimeout
	 *            the connect timeout
	 * @param timeUnit
	 *            the time unit for the connect timeout
	 *
	 * @see java.net.URLConnection#setConnectTimeout(int)
	 */
	void setConnectTimeout(long connectTimeout, TimeUnit timeUnit);

	/**
	 * Sets the default read timeout for new connections. A value of 0 means no timeout. If not
	 * set explicitly, a default timeout of READ_TIMEOUT_SECONDS_DEFAULT seconds will be used.
	 *
	 * @param readTimeout
	 *            the read timeout
	 * @param timeUnit
	 *            the time unit for the read timeout
	 *
	 * @see java.net.URLConnection#setReadTimeout(int)
	 */
	void setReadTimeout(long readTimeout, TimeUnit timeUnit);

	/**
	 * Sets the default write timeout for new connections. A value of 0 means no timeout. If not
	 * set explicitly, a default timeout of WRITE_TIMEOUT_SECONDS_DEFAULT seconds will be used.
	 *
	 * @param writeTimeout
	 *            the write timeout
	 * @param timeUnit
	 *            the time unit for the write timeout
	 */
	void setWriteTimeout(long writeTimeout, TimeUnit timeUnit);

}

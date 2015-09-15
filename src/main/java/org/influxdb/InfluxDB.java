package org.influxdb;

import org.influxdb.dto.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
   enum LogLevel {
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
		FULL
   }

	/**
	 * ConsistencyLevel for write Operations.
	 */
   enum ConsistencyLevel {
		/** Write succeeds only if write reached all cluster members. */
		ALL("all"),
		/** Write succeeds if write reached any cluster members. */
		ANY("any"),
		/** Write succeeds if write reached at least one cluster members. */
		ONE("one"),
		/** Write succeeds only if write reached a quorum of cluster members. */
		QUORUM("quorum");
		private final String value;

		ConsistencyLevel(String value) {
			this.value = value;
		}

		/**
		 * Get the String value of the ConsistencyLevel.
		 *
		 * @return the lowercase String.
		 */
		public String value() {
			return value;
		}
	}

	/**
	 * Set the loglevel which is used for REST related actions.
	 * 
	 * @param logLevel
	 *            the loglevel to set.
	 * @return the InfluxDB instance to be able to use it in a fluent manner.
	 */
   InfluxDB setLogLevel(LogLevel logLevel);

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
   InfluxDB enableBatch(int actions, int flushDuration, TimeUnit flushDurationTimeUnit);

	/**
	 * Disable Batching.
	 */
   void disableBatch();

	/**
	 * Ping this influxDB-
	 * 
	 * @return the response of the ping execution.
	 */
   Pong ping();

	/**
	 * Return the version of the connected influxDB Server.
	 * 
	 * @return the version String, otherwise unknown.
	 */
   String version();

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
   void write(String database, String retentionPolicy, Point point);

	/**
	 * Write a set of Points to the influxdb database with the new (>= 0.9.0rc32) lineprotocol.
	 * 
	 * {@linkplain "https://github.com/influxdb/influxdb/pull/2696"}
	 *
	 * @param batchPoints
	 */
   void write(BatchPoints batchPoints);

	/**
	 * Execute a query agains a database.
	 * 
	 * @param query
	 *            the query to execute.
	 * @return a List of Series which matched the query.
	 */
   QueryResult query(Query query);

	/**
	 * Execute a query agains a database.
	 * 
	 * @param query
	 *            the query to execute.
	 * @param timeUnit the time unit of the results. 
	 * @return a List of Series which matched the query.
	 */
   QueryResult query(Query query, TimeUnit timeUnit);

	/**
	 * Create a new Database.
	 *
	 * @param name
	 *            the name of the new database.
	 */
   void createDatabase(String name);

	/**
	 * Delete a database.
	 *
	 * @param name
	 *            the name of the database to delete.
	 */
   void deleteDatabase(String name);

	/**
	 * Describe all available databases.
	 *
	 * @return a List of all Database names.
	 */
   List<String> describeDatabases();

}
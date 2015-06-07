package org.influxdb;

import java.util.List;

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
	 * Write a set of Points to the database.
	 * 
	 * @param batchPoints
	 *            The batchPoints to write.
	 */
	public void write(final BatchPoints batchPoints);

	/**
	 * Write a set of Points to the database in a async fashion.
	 * 
	 * @param batchPoints
	 *            The batchPoints to write.
	 */
	public void writeAsync(final BatchPoints batchPoints);

	/**
	 * Execute a query agains a database.
	 * 
	 * @param query
	 *            the query to execute.
	 * @return a List of Series which matched the query.
	 */
	public QueryResult query(final Query query);

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

}
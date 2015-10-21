package org.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import com.google.common.base.Optional;

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
	 * Behaviour options for when a put fails to add to the buffer. This is
	 * particularly important when using capacity limited buffering.
	 */
	public enum BufferFailBehaviour {
		/** Throw an exception if cannot add to buffer */
		THROW_EXCEPTION,
		/** Drop (do not add) the element attempting to be added */
		DROP_CURRENT,
		/** Drop the oldest element in the queue and add the current element */
		DROP_OLDEST, 
		/** Block the thread until the space becomes available (NB: Not tested) */
		BLOCK_THREAD,
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
	 * Enable Batching of single Point with a capacity limit. Batching provides
	 * a significant performance improvement in write speed. If either actions
	 * or flushDurations is reached first, a batchwrite is issued.
	 * 
	 * This allows greater control over the behaviour when the capacity of the
	 * underlying buffer is limited.
	 * 
	 * @param capacity
	 *            the maximum number of points to hold. Should be NULL, for no
	 *            buffering OR > 0 for buffering (NB: a capacity of 1 will not
	 *            really buffer)
	 * @param actions
	 *            the number of actions to collect before triggering a batched
	 *            write
	 * @param flushDuration
	 *            the amount of time to wait, at most, before triggering a
	 *            batched write
	 * @param flushDurationTimeUnit
	 *            the time unit for the flushDuration parameter
	 * @param behaviour
	 *            the desired behaviour when capacity constrains are met
	 * @param discardOnFailedWrite
	 *            if FALSE, the points from a failed batch write buffer will
	 *            attempt to put them back onto the queue if TRUE, the points
	 *            froma failed batch write will be discarded
	 * @param maxBatchWriteSize
	 *            the maximum number of points to include in one batch write
	 *            attempt. NB: this is different from the actions parameter, as
	 *            the buffer can hold more than the actions parameter
	 * @return
	 */
	public InfluxDB enableBatch(final Integer capacity, final int actions,
			final int flushDuration, final TimeUnit flushDurationTimeUnit,
			BufferFailBehaviour behaviour, boolean discardOnFailedWrite,
			int maxBatchWriteSize);
	
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
	 * Write a single Point to the database with ConsistencyLevel.One.
	 * 
	 * @param database
	 *            the database to write to.
	 * @param retentionPolicy
	 *            the retentionPolicy to use.
	 * @param point
	 *            The point to write
	 */
	@Deprecated
	public void write(final String database, final String retentionPolicy, final Point point);
	
	/**
	 * Write a single Point to the database.
	 * 
	 * @param database
	 * @param retentionPolicy
	 * @param consistencyLevel
	 * @param point
	 */
	public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistencyLevel, final Point point);

	/**
	 * Write a set of Points to the influxdb database with the new (>= 0.9.0rc32) lineprotocol.
	 * 
	 * {@linkplain "https://github.com/influxdb/influxdb/pull/2696"}
	 * 
	 * @param batchPoints
	 */
	@Deprecated
	public void write(final BatchPoints batchPoints);
	
	/**
	 * Write a set of Points to the influxdb database with the new (>= 0.9.0rc32) lineprotocol.
	 * 
	 * {@linkplain "https://github.com/influxdb/influxdb/pull/2696"}
	 * 
	 * @param batchPoints
	 */
	public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistencyLevel, final List<Point> points);


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
	 * Get the number of buffered points NB: If batching is not enabled this
	 * will return 0
	 * 
	 * @return
	 */
	public int getBufferedCount();

	/**
	 * Retrieves, but does not remove, the first element of the buffer
	 * 
	 * @return an Optional<Point> containing the first element in the queue if
	 *         it is present
	 */
	public Optional<Point> peekFirstBuffered();

}
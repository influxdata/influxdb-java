package org.influxdb;

import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

  /**
   * The system property key to set the http logging level across the JVM.
   * @see LogLevel for available values
   */
  public static final String LOG_LEVEL_PROPERTY = "org.influxdb.InfluxDB.logLevel";

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
    /**
     * Parses the string argument as a LogLevel constant.
     * @param value a {@code String} containing the {@code LogLevel constant}
     *             representation to be parsed
     * @return the LogLevel constant representation of the param
     *  or {@code NONE} for null or any invalid String representation.
     */
    public static LogLevel parseLogLevel(final String value) {
      LogLevel logLevel = NONE;
      if (value != null) {
        try {
          logLevel = valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
        }
      }

      return logLevel;
    }
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
   * Format of HTTP Response body from InfluxDB server.
   */
  public enum ResponseFormat {
    /** application/json format. */
    JSON,
    /** application/x-msgpack format. */
    MSGPACK
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
   * Enable Gzip compress for http request body.
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB enableGzip();

  /**
   * Disable Gzip compress for http request body.
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB disableGzip();

  /**
   * Returns whether Gzip compress for http request body is enabled.
   * @return true if gzip is enabled.
   */
  public boolean isGzipEnabled();

  /**
   * Enable batching of single Point writes to speed up writes significantly. This is the same as calling
   * InfluxDB.enableBatch(BatchingOptions.DEFAULTS)
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB enableBatch();

  /**
   * Enable batching of single Point writes to speed up writes significantly. If either number of points written or
   * flushDuration time limit is reached, a batch write is issued.
   * Note that batch processing needs to be explicitly stopped before the application is shutdown.
   * To do so call disableBatch().
   *
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB enableBatch(final BatchOptions batchOptions);

  /**
   * Enable batching of single Point writes as {@link #enableBatch(int, int, TimeUnit, ThreadFactory)}}
   * using {@linkplain java.util.concurrent.Executors#defaultThreadFactory() default thread factory}.
   *
   * @param actions
   *            the number of actions to collect
   * @param flushDuration
   *            the time to wait at most.
   * @param flushDurationTimeUnit
   *            the TimeUnit for the given flushDuration.
   *
   * @see #enableBatch(int, int, TimeUnit, ThreadFactory)
   *
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit);

  /**
   * Enable batching of single Point writes as
   * {@link #enableBatch(int, int, TimeUnit, ThreadFactory, BiConsumer)}
   * using with a exceptionHandler that does nothing.
   *
   * @param actions
   *            the number of actions to collect
   * @param flushDuration
   *            the time to wait at most.
   * @param flushDurationTimeUnit
   *            the TimeUnit for the given flushDuration.
   * @param threadFactory
   *            a ThreadFactory instance to be used.
   *
   * @see #enableBatch(int, int, TimeUnit, ThreadFactory, BiConsumer)
   *
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit,
                              final ThreadFactory threadFactory);
  /**
   * Enable batching of single Point writes with consistency set for an entire batch
   * flushDurations is reached first, a batch write is issued.
   * Note that batch processing needs to be explicitly stopped before the application is shutdown.
   * To do so call disableBatch(). Default consistency is ONE.
   *
   * @param actions
   *            the number of actions to collect
   * @param flushDuration
   *            the time to wait at most.
   * @param flushDurationTimeUnit
   *            the TimeUnit for the given flushDuration.
   * @param threadFactory
   *            a ThreadFactory instance to be used.
   * @param exceptionHandler
   *            a consumer function to handle asynchronous errors
   * @param consistency
   *            a consistency setting for batch writes.
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */

  InfluxDB enableBatch(int actions, int flushDuration, TimeUnit flushDurationTimeUnit,
                       ThreadFactory threadFactory, BiConsumer<Iterable<Point>, Throwable> exceptionHandler,
                       ConsistencyLevel consistency);

  /**
   * Enable batching of single Point writes to speed up writes significant. If either actions or
   * flushDurations is reached first, a batch write is issued.
   * Note that batch processing needs to be explicitly stopped before the application is shutdown.
   * To do so call disableBatch().
   *
   * @param actions
   *            the number of actions to collect
   * @param flushDuration
   *            the time to wait at most.
   * @param flushDurationTimeUnit
   *            the TimeUnit for the given flushDuration.
   * @param threadFactory
   *            a ThreadFactory instance to be used.
   * @param exceptionHandler
   *            a consumer function to handle asynchronous errors
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit,
                              final ThreadFactory threadFactory,
                              final BiConsumer<Iterable<Point>, Throwable> exceptionHandler);

  /**
   * Disable Batching.
   */
  public void disableBatch();

  /**
   * Returns whether Batching is enabled.
   * @return true if batch is enabled.
   */
  public boolean isBatchEnabled();

  /**
   * Ping this influxDB.
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
   * Write a single Point to the default database.
   *
   * @param point
   *            The point to write
   */
  public void write(final Point point);

  /**
   * Write a set of Points to the default database with the string records.
   *
   * @param records
   *            the points in the correct lineprotocol.
   */
  public void write(final String records);

  /**
   * Write a set of Points to the default database with the list of string records.
   *
   * @param records
   *            the List of points in the correct lineprotocol.
   */
  public void write(final List<String> records);

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
   * Write a single Point to the database through UDP.
   *
   * @param udpPort
   *            the udpPort to write to.
   * @param point
   *            The point to write.
   */
  public void write(final int udpPort, final Point point);

  /**
   * Write a set of Points to the influxdb database with the new (&gt;= 0.9.0rc32) lineprotocol.
   *
   * @see <a href="https://github.com/influxdb/influxdb/pull/2696">2696</a>
   *
   * @param batchPoints
   *            the points to write in BatchPoints.
   */
  public void write(final BatchPoints batchPoints);

  /**
   * Write a set of Points to the influxdb database with the new (&gt;= 0.9.0rc32) lineprotocol.
   *
   * If batching is enabled with appropriate {@code BatchOptions} settings
   * ({@code BatchOptions.bufferLimit} greater than {@code BatchOptions.actions})
   * This method will try to retry in case of some recoverable errors.
   * Otherwise it just works as {@link #write(BatchPoints)}
   *
   * @see <a href="https://github.com/influxdb/influxdb/pull/2696">2696</a>
   * @see <a href="https://github.com/influxdata/influxdb-java/wiki/Handling-errors-of-InfluxDB-under-high-load">
   * Retry worth errors</a>
   *
   * @param batchPoints
   *            the points to write in BatchPoints.
   */
  public void writeWithRetry(final BatchPoints batchPoints);

  /**
   * Write a set of Points to the influxdb database with the string records.
   *
   * @see <a href="https://github.com/influxdb/influxdb/pull/2696">2696</a>
   *
   * @param database
   *          the name of the database to write
   * @param retentionPolicy
   *          the retentionPolicy to use
   * @param consistency
   *          the ConsistencyLevel to use
   * @param records
   *            the points in the correct lineprotocol.
   */
  public void write(final String database, final String retentionPolicy,
                    final ConsistencyLevel consistency, final String records);

  /**
   * Write a set of Points to the influxdb database with the string records.
   *
   * @see <a href="https://github.com/influxdb/influxdb/pull/2696">2696</a>
   *
   * @param database
   *          the name of the database to write
   * @param retentionPolicy
   *          the retentionPolicy to use
   * @param consistency
   *          the ConsistencyLevel to use
   * @param precision
   *          the time precision to use
   * @param records
   *            the points in the correct lineprotocol.
   */
  public void write(final String database, final String retentionPolicy,
          final ConsistencyLevel consistency, final TimeUnit precision, final String records);

  /**
   * Write a set of Points to the influxdb database with the list of string records.
   *
   * @see <a href="https://github.com/influxdb/influxdb/pull/2696">2696</a>
   *
   * @param database
   *          the name of the database to write
   * @param retentionPolicy
   *          the retentionPolicy to use
   * @param consistency
   *          the ConsistencyLevel to use
   * @param records
   *          the List of points in the correct lineprotocol.
   */
  public void write(final String database, final String retentionPolicy,
                    final ConsistencyLevel consistency, final List<String> records);

  /**
   * Write a set of Points to the influxdb database with the list of string records.
   *
   * @see <a href="https://github.com/influxdb/influxdb/pull/2696">2696</a>
   *
   * @param database
   *          the name of the database to write
   * @param retentionPolicy
   *          the retentionPolicy to use
   * @param consistency
   *          the ConsistencyLevel to use
   * @param precision
   *          the time precision to use
   * @param records
   *          the List of points in the correct lineprotocol.
   */
  public void write(final String database, final String retentionPolicy,
          final ConsistencyLevel consistency, final TimeUnit precision, final List<String> records);

  /**
   * Write a set of Points to the influxdb database with the string records through UDP.
   *
   * @param udpPort
  *           the udpPort where influxdb is listening
   * @param records
   *          the content will be encoded by UTF-8 before sent.
   */
  public void write(final int udpPort, final String records);

  /**
   * Write a set of Points to the influxdb database with the list of string records through UDP.
   *
   * @param udpPort
   *           the udpPort where influxdb is listening
   * @param records
   *           list of record, the content will be encoded by UTF-8 before sent.
   */
  public void write(final int udpPort, final List<String> records);

  /**
   * Execute a query against a database.
   *
   * @param query
   *            the query to execute.
   * @return a List of Series which matched the query.
   */
  public QueryResult query(final Query query);

  /**
   * Execute a query against a database.
   *
   * One of the consumers will be executed.
   *
   * @param query
   *            the query to execute.
   * @param onSuccess
   *            the consumer to invoke when result is received
   * @param onFailure
   *            the consumer to invoke when error is thrown
   */
  public void query(final Query query, final Consumer<QueryResult> onSuccess, final Consumer<Throwable> onFailure);

  /**
   * Execute a streaming query against a database.
   *
   * @param query
   *            the query to execute.
   * @param chunkSize
   *            the number of QueryResults to process in one chunk.
   * @param onNext
   *            the consumer to invoke for each received QueryResult
   */
  public void query(Query query, int chunkSize, Consumer<QueryResult> onNext);

  /**
   * Execute a streaming query against a database.
   *
   * @param query
   *            the query to execute.
   * @param chunkSize
   *            the number of QueryResults to process in one chunk.
   * @param onNext
   *            the consumer to invoke for each received QueryResult
   * @param onComplete
   *            the onComplete to invoke for successfully end of stream
   */
  public void query(Query query, int chunkSize, Consumer<QueryResult> onNext, Runnable onComplete);

  /**
   * Execute a query against a database.
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
   * @deprecated (since 2.9, removed in 3.0) Use <tt>org.influxdb.InfluxDB.query(Query)</tt> to execute a parameterized
   *             <strong>CREATE DATABASE</strong> query.
   */
  @Deprecated
  public void createDatabase(final String name);

  /**
   * Delete a database.
   *
   * @param name
   *            the name of the database to delete.
   * @deprecated (since 2.9, removed in 3.0) Use <tt>org.influxdb.InfluxDB.query(Query)</tt> to execute a
   *             <strong>DROP DATABASE</strong> query.
   */
  @Deprecated
  public void deleteDatabase(final String name);

  /**
   * Describe all available databases.
   *
   * @return a List of all Database names.
   * @deprecated (since 2.9, removed in 3.0) Use <tt>org.influxdb.InfluxDB.query(Query)</tt> to execute a
   *             <strong>SHOW DATABASES</strong> query.
   */
  @Deprecated
  public List<String> describeDatabases();

  /**
   * Check if a database exists.
   *
   * @param name
   *            the name of the database to search.
   *
   * @return true if the database exists or false if it doesn't exist
   * @deprecated (since 2.9, removed in 3.0) Use <tt>org.influxdb.InfluxDB.query(Query)</tt> to execute a
   *             <strong>SHOW DATABASES</strong> query and inspect the result.
   */
  @Deprecated
  public boolean databaseExists(final String name);

  /**
   * Send any buffered points to InfluxDB. This method is synchronous and will block while all pending points are
   * written.
   *
   * @throws IllegalStateException if batching is not enabled.
   */
  public void flush();

  /**
   * close thread for asynchronous batch write and UDP socket to release resources if need.
   */
  public void close();

  /**
   * Set the consistency level which is used for writing points.
   *
   * @param consistency
   *            the consistency level to set.
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB setConsistency(final ConsistencyLevel consistency);

  /**
   * Set the database which is used for writing points.
   *
   * @param database
   *            the database to set.
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB setDatabase(final String database);

  /**
   * Set the retention policy which is used for writing points.
   *
   * @param retentionPolicy
   *            the retention policy to set.
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB setRetentionPolicy(final String retentionPolicy);

  /**
   * Creates a retentionPolicy.
   * @param rpName the name of the retentionPolicy(rp)
   * @param database the name of the database
   * @param duration the duration of the rp
   * @param shardDuration the shardDuration
   * @param replicationFactor the replicationFactor of the rp
   * @param isDefault if the rp is the default rp for the database or not
   * @deprecated (since 2.9, removed in 3.0) Use <tt>org.influxdb.InfluxDB.query(Query)</tt> to execute a parameterized
   *             <strong>CREATE RETENTION POLICY</strong> query.
   */
  @Deprecated
  public void createRetentionPolicy(final String rpName, final String database, final String duration,
                                    final String shardDuration, final int replicationFactor, final boolean isDefault);

  /**
   * Creates a retentionPolicy. (optional shardDuration)
   * @param rpName the name of the retentionPolicy(rp)
   * @param database the name of the database
   * @param duration the duration of the rp
   * @param replicationFactor the replicationFactor of the rp
   * @param isDefault if the rp is the default rp for the database or not
   * @deprecated (since 2.9, removed in 3.0) Use <tt>org.influxdb.InfluxDB.query(Query)</tt> to execute a parameterized
   *             <strong>CREATE RETENTION POLICY</strong> query.
   */
  @Deprecated
  public void createRetentionPolicy(final String rpName, final String database, final String duration,
                                    final int replicationFactor, final boolean isDefault);

  /**
   * Creates a retentionPolicy. (optional shardDuration and isDefault)
   * @param rpName the name of the retentionPolicy(rp)
   * @param database the name of the database
   * @param duration the duration of the rp
   * @param shardDuration the shardDuration
   * @param replicationFactor the replicationFactor of the rp
   * @deprecated (since 2.9, removed in 3.0) Use <tt>org.influxdb.InfluxDB.query(Query)</tt> to execute a parameterized
   *             <strong>CREATE RETENTION POLICY</strong> query.
   */
  @Deprecated
  public void createRetentionPolicy(final String rpName, final String database, final String duration,
                                    final String shardDuration, final int replicationFactor);

  /**
   * Drops a retentionPolicy in a database.
   * @param rpName the name of the retentionPolicy
   * @param database the name of the database
   * @deprecated (since 2.9, removed in 3.0) Use <tt>org.influxdb.InfluxDB.query(Query)</tt> to execute a
   *             <strong>DROP RETENTION POLICY</strong> query.
   */
  @Deprecated
  public void dropRetentionPolicy(final String rpName, final String database);
}

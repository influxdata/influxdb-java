package org.influxdb;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.exception.DeleteInfluxException;

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
   * Set the loglevel which is used for REST related actions.
   *
   * @param logLevel
   *            the loglevel to set.
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB setLogLevel(final LogLevel logLevel);

  /**
   * Enable Gzip compress for http request body.
   */
  public InfluxDB enableGzip();

  /**
   * Disable Gzip compress for http request body.
   */
  public InfluxDB disableGzip();

  /**
   * Returns whether Gzip compress for http request body is enabled.
   */
  public boolean isGzipEnabled();

  /**
   * Enable batching of single Point writes as {@link #enableBatch(int, int, TimeUnit, ThreadFactory)}}
   * using {@linkplain java.util.concurrent.Executors#defaultThreadFactory() default thread factory}.
   *
   * @see #enableBatch(int, int, TimeUnit, ThreadFactory)
   */
  public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit);

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
   * @param threadFactory
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit,
                              final ThreadFactory threadFactory);

  /**
   * Disable Batching.
   */
  public void disableBatch();

  /**
   * Returns whether Batching is enabled.
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

  void dropMeasurement(String database, String measurement) throws DeleteInfluxException;

  /**
   *
   * @param database - name database
   * @param point - point for delete by tags
   * @throws DeleteInfluxException
   */
  void delete(String database, Point point) throws DeleteInfluxException;

    /**
     * Delete data by time: "WHERE time < now()".
     *
     * @param database    - name database
     * @param measurement - where we will delete data
     * @throws DeleteInfluxException
     */
  void deleteOld(String database, String measurement) throws DeleteInfluxException;

    /**
     * Delete data by time: "WHERE time < date".
     *
     * @param database    - name database
     * @param measurement - where we will delete data
     * @param date        - where we will delete data
     * @throws DeleteInfluxException
     */
  void deleteBeforeDate(String database, String measurement, Date date) throws DeleteInfluxException;

    /**
     * Delete data by time: "WHERE time > date".
     *
     * @param database    - name database
     * @param measurement - where we will delete data
     * @param date        - where we will delete data
     * @throws DeleteInfluxException
     */
  void deleteAfterDate(String database, String measurement, Date date) throws DeleteInfluxException;

  /**
   * Write a set of Points to the influxdb database with the new (>= 0.9.0rc32) lineprotocol.
   *
   * {@linkplain "https://github.com/influxdb/influxdb/pull/2696"}
   *
   * @param batchPoints
   */
  public void write(final BatchPoints batchPoints);

  /**
   * Write a set of Points to the influxdb database with the string records.
   *
   * {@linkplain "https://github.com/influxdb/influxdb/pull/2696"}
   *
   * @param records
   */
  public void write(final String database, final String retentionPolicy,
                    final ConsistencyLevel consistency, final String records);

  /**
   * Write a set of Points to the influxdb database with the list of string records.
   *
   * {@linkplain "https://github.com/influxdb/influxdb/pull/2696"}
   *
   * @param records
   */
  public void write(final String database, final String retentionPolicy,
                    final ConsistencyLevel consistency, final List<String> records);

  /**
   * Write a set of Points to the influxdb database with the string records through UDP.
   *
   * @param udpPort
   * @param records the content will be encoded by UTF-8 before sent.
   */
  public void write(final int udpPort, final String records);

  /**
   * Write a set of Points to the influxdb database with the list of string records through UDP.
   *
   * @param udpPort
   * @param records list of record, the content will be encoded by UTF-8 before sent.
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
   * Execute a streaming query against a database.
   *
   * @param query
   *            the query to execute.
   * @param chunkSize
   *            the number of QueryResults to process in one chunk.
   * @param consumer
   *            the consumer to invoke for each received QueryResult
   */
    public void query(Query query, int chunkSize, Consumer<QueryResult> consumer);

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
   * Check if a database exists.
   *
   * @param name
   *            the name of the database to search.
   *
   * @return true if the database exists or false if it doesn't exist
   */
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

}

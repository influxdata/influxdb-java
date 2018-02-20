package org.influxdb;

import org.influxdb.dto.Point;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

/**
 * BatchOptions are used to configure batching of individual data point writes
 * into InfluxDB. See {@link InfluxDB#enableBatch(BatchOptions)}
 */
public final class BatchOptions implements Cloneable {

  /**
   * Default batch options. This class is immutable, each configuration
   * is built by taking the DEFAULTS and setting specific configuration
   * properties.
   */
  public static final BatchOptions DEFAULTS = new BatchOptions();

  // default values here are consistent with Telegraf
  public static final int DEFAULT_BATCH_ACTIONS_LIMIT = 1000;
  public static final int DEFAULT_BATCH_INTERVAL_DURATION = 1000;
  public static final int DEFAULT_JITTER_INTERVAL_DURATION = 0;
  public static final int DEFAULT_BUFFER_LIMIT = 10000;

  private int actions = DEFAULT_BATCH_ACTIONS_LIMIT;
  private int flushDuration = DEFAULT_BATCH_INTERVAL_DURATION;
  private int jitterDuration = DEFAULT_JITTER_INTERVAL_DURATION;
  private int bufferLimit = DEFAULT_BUFFER_LIMIT;

  private ThreadFactory threadFactory = Executors.defaultThreadFactory();
  BiConsumer<Iterable<Point>, Throwable> exceptionHandler = (points, throwable) -> {
  };
  private InfluxDB.ConsistencyLevel consistency = InfluxDB.ConsistencyLevel.ONE;

  private BatchOptions() {
  }

  /**
   * @param actions the number of actions to collect
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  public BatchOptions actions(final int actions) {
    BatchOptions clone = getClone();
    clone.actions = actions;
    return clone;
  }

  /**
   * @param flushDuration the time to wait at most (milliseconds).
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  public BatchOptions flushDuration(final int flushDuration) {
    BatchOptions clone = getClone();
    clone.flushDuration = flushDuration;
    return clone;
  }

  /**
   * Jitters the batch flush interval by a random amount. This is primarily to avoid
   * large write spikes for users running a large number of client instances.
   * ie, a jitter of 5s and flush duration 10s means flushes will happen every 10-15s.
   *
   * @param jitterDuration (milliseconds)
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  public BatchOptions jitterDuration(final int jitterDuration) {
    BatchOptions clone = getClone();
    clone.jitterDuration = jitterDuration;
    return clone;
  }

  /**
   * The client maintains a buffer for failed writes so that the writes will be retried later on. This may
   * help to overcome temporary network problems or InfluxDB load spikes.
   * When the buffer is full and new points are written, oldest entries in the buffer are lost.
   *
   * To disable this feature set buffer limit to a value smaller than {@link BatchOptions#getActions}
   *
   * @param bufferLimit maximum number of points stored in the retry buffer
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  public BatchOptions bufferLimit(final int bufferLimit) {
    BatchOptions clone = getClone();
    clone.bufferLimit = bufferLimit;
    return clone;
  }

  /**
   * @param threadFactory a ThreadFactory instance to be used
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  public BatchOptions threadFactory(final ThreadFactory threadFactory) {
    BatchOptions clone = getClone();
    clone.threadFactory = threadFactory;
    return clone;
  }

  /**
   * @param exceptionHandler a consumer function to handle asynchronous errors
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  public BatchOptions exceptionHandler(final BiConsumer<Iterable<Point>, Throwable> exceptionHandler) {
    BatchOptions clone = getClone();
    clone.exceptionHandler = exceptionHandler;
    return clone;
  }

  /**
   * @param consistency cluster consistency setting (how many nodes have to store data points
   *                    to treat a write as a success)
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  public BatchOptions consistency(final InfluxDB.ConsistencyLevel consistency) {
    BatchOptions clone = getClone();
    clone.consistency = consistency;
    return clone;
  }

  /**
   * @return actions the number of actions to collect
   */
  public int getActions() {
    return actions;
  }

  /**
   * @return flushDuration the time to wait at most (milliseconds).
   */
  public int getFlushDuration() {
    return flushDuration;
  }

  /**
   * @return batch flush interval jitter value (milliseconds)
   */
  public int getJitterDuration() {
    return jitterDuration;
  }

  /**
   * @return Maximum number of points stored in the retry buffer, see {@link BatchOptions#bufferLimit(int)}
   */
  public int getBufferLimit() {
    return bufferLimit;
  }

  /**
   * @return a ThreadFactory instance to be used
   */
  public ThreadFactory getThreadFactory() {
    return threadFactory;
  }

  /**
   * @return a consumer function to handle asynchronous errors
   */
  public BiConsumer<Iterable<Point>, Throwable> getExceptionHandler() {
    return exceptionHandler;
  }

  /**
   * @return cluster consistency setting (how many nodes have to store data points
   * to treat a write as a success)
   */
  public InfluxDB.ConsistencyLevel getConsistency() {
    return consistency;
  }

  private BatchOptions getClone() {
    try {
      return (BatchOptions) this.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

}

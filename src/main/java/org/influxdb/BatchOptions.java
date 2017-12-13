package org.influxdb;

import org.influxdb.dto.Point;
import org.influxdb.impl.BatchOptionsImpl;

import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

/**
 * BatchOptions are used to configure batching of individual data point writes
 * into InfluxDB. See {@link InfluxDB#enableBatch(BatchOptions)}
 */
public interface BatchOptions {

  BatchOptions DEFAULTS = BatchOptionsImpl.DEFAULTS;

  /**
   * @param actions the number of actions to collect
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  BatchOptions actions(final int actions);

  /**
   * @param flushDuration the time to wait at most (milliseconds).
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  BatchOptions flushDuration(final int flushDuration);

  /**
   * Jitters the batch flush interval by a random amount. This is primarily to avoid
   * large write spikes for users running a large number of client instances.
   * ie, a jitter of 5s and flush duration 10s means flushes will happen every 10-15s.
   *
   * @param jitterDuration (milliseconds)
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  BatchOptions jitterDuration(final int jitterDuration);

  /**
   * @param threadFactory a ThreadFactory instance to be used
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  BatchOptions threadFactory(final ThreadFactory threadFactory);

  /**
   * @param exceptionHandler a consumer function to handle asynchronous errors
   * @return the BatchOptions instance to be able to use it in a fluent manner.
   */
  BatchOptions exceptionHandler(final BiConsumer<Iterable<Point>, Throwable> exceptionHandler);

  /**
   * @return actions the number of actions to collect
   */
  int getActions();

  /**
   * @return flushDuration the time to wait at most (milliseconds).
   */
  int getFlushDuration();

  /**
   * @return batch flush interval jitter value (milliseconds)
   */
  int getJitterDuration();

  /**
   * @return a ThreadFactory instance to be used
   */
  ThreadFactory getThreadFactory();

  /**
   * @return a consumer function to handle asynchronous errors
   */
  BiConsumer<Iterable<Point>, Throwable> getExceptionHandler();
}

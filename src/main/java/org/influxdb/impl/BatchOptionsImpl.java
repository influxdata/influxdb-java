package org.influxdb.impl;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

public final class BatchOptionsImpl implements BatchOptions, Cloneable {

  public static final BatchOptions DEFAULTS = new BatchOptionsImpl();

  // default values here are consistent with Telegraf
  public static final int DEFAULT_BATCH_ACTIONS_LIMIT = 1000;
  public static final int DEFAULT_BATCH_INTERVAL_DURATION = 1000;
  public static final int DEFAULT_JITTER_INTERVAL_DURATION = 0;
  public static final int DEFAULT_BUFFER_LIMIT = 10000;

  private int actions = DEFAULT_BATCH_ACTIONS_LIMIT;
  private int flushDuration = DEFAULT_BATCH_INTERVAL_DURATION;
  private int jitterDuration = DEFAULT_JITTER_INTERVAL_DURATION;
  private int bufferLimit = DEFAULT_BUFFER_LIMIT;

  ThreadFactory threadFactory = Executors.defaultThreadFactory();
  BiConsumer<Iterable<Point>, Throwable> exceptionHandler = (points, throwable) -> {
  };
  InfluxDB.ConsistencyLevel consistency = InfluxDB.ConsistencyLevel.ONE;

  private BatchOptionsImpl() {
  }

  public BatchOptions actions(final int actions) {
    BatchOptionsImpl clone = getClone();
    clone.actions = actions;
    return clone;
  }

  public BatchOptions flushDuration(final int flushDuration) {
    BatchOptionsImpl clone = getClone();
    clone.flushDuration = flushDuration;
    return clone;
  }

  public BatchOptions jitterDuration(final int jitterDuration) {
    BatchOptionsImpl clone = getClone();
    clone.jitterDuration = jitterDuration;
    return clone;
  }

  public BatchOptions bufferLimit(final int bufferLimit) {
    BatchOptionsImpl clone = getClone();
    clone.bufferLimit = bufferLimit;
    return clone;
  }

  public BatchOptions threadFactory(final ThreadFactory threadFactory) {
    BatchOptionsImpl clone = getClone();
    clone.threadFactory = threadFactory;
    return clone;
  }

  public BatchOptions exceptionHandler(final BiConsumer<Iterable<Point>, Throwable> exceptionHandler) {
    BatchOptionsImpl clone = getClone();
    clone.exceptionHandler = exceptionHandler;
    return clone;
  }

  public BatchOptions consistency(final InfluxDB.ConsistencyLevel consistency) {
    BatchOptionsImpl clone = getClone();
    clone.consistency = consistency;
    return clone;
  }

  private BatchOptionsImpl getClone() {
    try {
      return (BatchOptionsImpl) this.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public int getActions() {
    return actions;
  }

  public int getFlushDuration() {
    return flushDuration;
  }

  public int getJitterDuration() {
    return jitterDuration;
  }

  public InfluxDB.ConsistencyLevel getConsistency() {
    return consistency;
  }

  public ThreadFactory getThreadFactory() {
    return threadFactory;
  }

  public BiConsumer<Iterable<Point>, Throwable> getExceptionHandler() {
    return exceptionHandler;
  }

  @Override
  public int getBufferLimit() {
    return bufferLimit;
  }
}

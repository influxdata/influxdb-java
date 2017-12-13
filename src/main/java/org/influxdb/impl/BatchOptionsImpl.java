package org.influxdb.impl;

import org.influxdb.BatchOptions;
import org.influxdb.dto.Point;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

public class BatchOptionsImpl implements BatchOptions, Cloneable {

  public static BatchOptions DEFAULTS = new BatchOptionsImpl();

  // default values here are consistent with Telegraf
  int actions = 1000;
  int flushDuration = 10000;
  ThreadFactory threadFactory = Executors.defaultThreadFactory();
  BiConsumer<Iterable<Point>, Throwable> exceptionHandler = (points, throwable) -> {
  };

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

  public ThreadFactory getThreadFactory() {
    return threadFactory;
  }

  public BiConsumer<Iterable<Point>, Throwable> getExceptionHandler() {
    return exceptionHandler;
  }
}

package org.influxdb.impl;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiConsumer;

/**
 * Batch writer that tries to retry a write if it failed previously and
 * the reason of the failure is not permanent.
 */
class RetryCapableBatchWriter implements BatchWriter {

  private InfluxDB influxDB;
  private BiConsumer<Iterable<Point>, Throwable> exceptionHandler;
  private LinkedList<BatchPoints> batchQueue;
  private int requestActionsLimit;
  private int retryBufferCapacity;
  private int usedRetryBufferCapacity;

  RetryCapableBatchWriter(final InfluxDB influxDB, final BiConsumer<Iterable<Point>, Throwable> exceptionHandler,
                          final int retryBufferCapacity, final int requestActionsLimit) {
    this.influxDB = influxDB;
    this.exceptionHandler = exceptionHandler;
    batchQueue = new LinkedList<>();
    this.retryBufferCapacity = retryBufferCapacity;
    this.requestActionsLimit = requestActionsLimit;
  }

  private enum WriteResultOutcome { WRITTEN, FAILED_RETRY_POSSIBLE, FAILED_RETRY_IMPOSSIBLE }

  private static class WriteResult {

    static final WriteResult WRITTEN = new WriteResult(WriteResultOutcome.WRITTEN);

    WriteResultOutcome outcome;
    Throwable throwable;

    public WriteResult(final WriteResultOutcome outcome) {
      this.outcome = outcome;
    }

    public WriteResult(final WriteResultOutcome outcome, final Throwable throwable) {
      this.outcome = outcome;
      this.throwable = throwable;
    }

    public WriteResult(final InfluxDBException e) {
      this.throwable = e;
      if (e.isRetryWorth()) {
        this.outcome = WriteResultOutcome.FAILED_RETRY_POSSIBLE;
      } else {
        this.outcome = WriteResultOutcome.FAILED_RETRY_IMPOSSIBLE;
      }
    }
  }

  @Override
  public void write(final BatchPoints batchPoints) {
    // empty the cached data first
    ListIterator<BatchPoints> iterator = batchQueue.listIterator();
    while (iterator.hasNext()) {
      BatchPoints entry = iterator.next();
      WriteResult result = tryToWrite(entry);
      if (result.outcome == WriteResultOutcome.WRITTEN
              || result.outcome == WriteResultOutcome.FAILED_RETRY_IMPOSSIBLE) {
        iterator.remove();
        usedRetryBufferCapacity -= entry.getPoints().size();
        // we are throwing out data, notify the client
        if (result.outcome == WriteResultOutcome.FAILED_RETRY_IMPOSSIBLE)
          exceptionHandler.accept(entry.getPoints(), result.throwable);
      } else {
        // we cannot send more data otherwise we would write them in different
        // order than in which were submitted
        addToBatchQueue(batchPoints);
        return;
      }
    }
    // write the last given batch last so that duplicate data points get overwritten correctly
    WriteResult result = tryToWrite(batchPoints);
    if (result.outcome == WriteResultOutcome.FAILED_RETRY_POSSIBLE) {
      addToBatchQueue(batchPoints);
    }
  }

  private WriteResult tryToWrite(final BatchPoints batchPoints) {
    try {
      influxDB.write(batchPoints);
      return WriteResult.WRITTEN;
    } catch (InfluxDBException e) {
      return new WriteResult(e);
    } catch (Exception e) {
      return new WriteResult(WriteResultOutcome.FAILED_RETRY_POSSIBLE, e);
    }
  }

  private void evictTooOldFailedWrites() {
    while (usedRetryBufferCapacity > retryBufferCapacity && batchQueue.size() > 0) {
      List<Point> points = batchQueue.removeFirst().getPoints();
      usedRetryBufferCapacity -= points.size();
      exceptionHandler.accept(points,
              new InfluxDBException.RetryBufferOverrunException(
                      "Retry buffer overrun, current capacity: " + retryBufferCapacity));
    }
  }

  private void addToBatchQueue(final BatchPoints batchPoints) {
    if (batchQueue.size() > 0) {
      BatchPoints last = batchQueue.getLast();
      if (last.getPoints().size() + batchPoints.getPoints().size() <= requestActionsLimit) {
        boolean hasBeenMergedIn = last.mergeIn(batchPoints);
        if (hasBeenMergedIn) {
          return;
        }
      }
    }
    batchQueue.add(batchPoints);
    usedRetryBufferCapacity += batchPoints.getPoints().size();
    evictTooOldFailedWrites();
  }
}

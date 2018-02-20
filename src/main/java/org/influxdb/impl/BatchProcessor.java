package org.influxdb.impl;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A BatchProcessor can be attached to a InfluxDB Instance to collect single point writes and
 * aggregates them to BatchPoints to get a better write performance.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public final class BatchProcessor {

  private static final Logger LOG = Logger.getLogger(BatchProcessor.class.getName());
  protected final BlockingQueue<AbstractBatchEntry> queue;
  private final ScheduledExecutorService scheduler;
  private final BiConsumer<Iterable<Point>, Throwable> exceptionHandler;
  final InfluxDBImpl influxDB;
  final int actions;
  private final TimeUnit flushIntervalUnit;
  private final int flushInterval;
  private final ConsistencyLevel consistencyLevel;
  private final int jitterInterval;
  private final BatchWriter batchWriter;

  /**
   * The Builder to create a BatchProcessor instance.
   */
  public static final class Builder {
    private final InfluxDBImpl influxDB;
    private ThreadFactory threadFactory = Executors.defaultThreadFactory();
    private int actions;
    private TimeUnit flushIntervalUnit;
    private int flushInterval;
    private int jitterInterval;
    // this is a default value if the InfluxDb.enableBatch(BatchOptions) IS NOT used
    // the reason is backward compatibility
    private int bufferLimit = 0;

    private BiConsumer<Iterable<Point>, Throwable> exceptionHandler = (entries, throwable) -> { };
    private ConsistencyLevel consistencyLevel;

    /**
     * @param threadFactory
     *            is optional.
     * @return this Builder to use it fluent
     */
    public Builder threadFactory(final ThreadFactory threadFactory) {
      this.threadFactory = threadFactory;
      return this;
    }

    /**
     * @param influxDB
     *            is mandatory.
     */
    public Builder(final InfluxDB influxDB) {
      this.influxDB = (InfluxDBImpl) influxDB;
    }

    /**
     * The number of actions after which a batchwrite must be performed.
     *
     * @param maxActions
     *            number of Points written after which a write must happen.
     * @return this Builder to use it fluent
     */
    public Builder actions(final int maxActions) {
      this.actions = maxActions;
      return this;
    }

    /**
     * The interval at which at least should issued a write.
     *
     * @param interval
     *            the interval
     * @param unit
     *            the TimeUnit of the interval
     *
     * @return this Builder to use it fluent
     */
    public Builder interval(final int interval, final TimeUnit unit) {
      this.flushInterval = interval;
      this.flushIntervalUnit = unit;
      return this;
    }

    /**
     * The interval at which at least should issued a write.
     *
     * @param flushInterval
     *            the flush interval
     * @param jitterInterval
     *            the flush jitter interval
     * @param unit
     *            the TimeUnit of the interval
     *
     * @return this Builder to use it fluent
     */
    public Builder interval(final int flushInterval, final int jitterInterval, final TimeUnit unit) {
      this.flushInterval = flushInterval;
      this.jitterInterval = jitterInterval;
      this.flushIntervalUnit = unit;
      return this;
    }

    /**
     * A buffer for failed writes so that the writes will be retried later on. When the buffer is full and
     * new points are written, oldest entries in the buffer are lost.
     *
     * @param bufferLimit maximum number of points stored in the buffer
     * @return this Builder to use it fluent
     */
    public Builder bufferLimit(final int bufferLimit) {
      this.bufferLimit = bufferLimit;
      return this;
    }

    /**
     * A callback to be used when an error occurs during a batchwrite.
     *
     * @param handler
     *            the handler
     *
     * @return this Builder to use it fluent
     */
    public Builder exceptionHandler(final BiConsumer<Iterable<Point>, Throwable> handler) {
      this.exceptionHandler = handler;
      return this;
    }
      /**
       * Consistency level for batch write.
       *
       * @param consistencyLevel
       *            the consistencyLevel
       *
       * @return this Builder to use it fluent
       */
      public Builder consistencyLevel(final ConsistencyLevel consistencyLevel) {
          this.consistencyLevel = consistencyLevel;
          return this;
      }

    /**
     * Create the BatchProcessor.
     *
     * @return the BatchProcessor instance.
     */
    public BatchProcessor build() {
      Objects.requireNonNull(this.influxDB, "influxDB");
      Preconditions.checkPositiveNumber(this.actions, "actions");
      Preconditions.checkPositiveNumber(this.flushInterval, "flushInterval");
      Preconditions.checkNotNegativeNumber(jitterInterval, "jitterInterval");
      Preconditions.checkNotNegativeNumber(bufferLimit, "bufferLimit");
      Objects.requireNonNull(this.flushIntervalUnit, "flushIntervalUnit");
      Objects.requireNonNull(this.threadFactory, "threadFactory");
      Objects.requireNonNull(this.exceptionHandler, "exceptionHandler");
      BatchWriter batchWriter;
      if (this.bufferLimit > this.actions) {
        batchWriter = new RetryCapableBatchWriter(this.influxDB, this.exceptionHandler, this.bufferLimit, this.actions);
      } else {
        batchWriter = new OneShotBatchWriter(this.influxDB);
      }
      return new BatchProcessor(this.influxDB, batchWriter, this.threadFactory, this.actions, this.flushIntervalUnit,
                                this.flushInterval, this.jitterInterval, exceptionHandler, this.consistencyLevel);
    }
  }

  abstract static class AbstractBatchEntry {
      private final Point point;

      public AbstractBatchEntry(final Point point) {
        this.point = point;
      }

      public Point getPoint() {
        return this.point;
      }
  }

  static class HttpBatchEntry extends AbstractBatchEntry {
    private final String db;
    private final String rp;

    public HttpBatchEntry(final Point point, final String db, final String rp) {
      super(point);
      this.db = db;
      this.rp = rp;
    }

    public String getDb() {
      return this.db;
    }

    public String getRp() {
      return this.rp;
    }
  }

  static class UdpBatchEntry extends AbstractBatchEntry {
      private final int udpPort;

      public UdpBatchEntry(final Point point, final int udpPort) {
        super(point);
        this.udpPort = udpPort;
      }

      public int getUdpPort() {
        return this.udpPort;
      }
  }

  /**
   * Static method to create the Builder for this BatchProcessor.
   *
   * @param influxDB
   *            the influxdb database handle.
   * @return the Builder to create the BatchProcessor.
   */
  public static Builder builder(final InfluxDB influxDB) {
    return new Builder(influxDB);
  }

  BatchProcessor(final InfluxDBImpl influxDB, final BatchWriter batchWriter, final ThreadFactory threadFactory,
                 final int actions, final TimeUnit flushIntervalUnit, final int flushInterval, final int jitterInterval,
                 final BiConsumer<Iterable<Point>, Throwable> exceptionHandler,
                 final ConsistencyLevel consistencyLevel) {
    super();
    this.influxDB = influxDB;
    this.batchWriter = batchWriter;
    this.actions = actions;
    this.flushIntervalUnit = flushIntervalUnit;
    this.flushInterval = flushInterval;
    this.jitterInterval = jitterInterval;
    this.scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    this.exceptionHandler = exceptionHandler;
    this.consistencyLevel = consistencyLevel;
    if (actions > 1 && actions < Integer.MAX_VALUE) {
        this.queue = new LinkedBlockingQueue<>(actions);
    } else {
        this.queue = new LinkedBlockingQueue<>();
    }

    Runnable flushRunnable = new Runnable() {
      @Override
      public void run() {
        // write doesn't throw any exceptions
        write();
        int jitterInterval = (int) (Math.random() * BatchProcessor.this.jitterInterval);
        BatchProcessor.this.scheduler.schedule(this,
                BatchProcessor.this.flushInterval + jitterInterval, BatchProcessor.this.flushIntervalUnit);
      }
    };
    // Flush at specified Rate
    this.scheduler.schedule(flushRunnable,
            this.flushInterval + (int) (Math.random() * BatchProcessor.this.jitterInterval),
            this.flushIntervalUnit);
  }

  void write() {
    List<Point> currentBatch = null;
    try {
      if (this.queue.isEmpty()) {
        BatchProcessor.this.batchWriter.write(Collections.emptyList());
        return;
      }
      //for batch on HTTP.
      Map<String, BatchPoints> batchKeyToBatchPoints = new HashMap<>();
      //for batch on UDP.
      Map<Integer, List<String>> udpPortToBatchPoints = new HashMap<>();
      List<AbstractBatchEntry> batchEntries = new ArrayList<>(this.queue.size());
      this.queue.drainTo(batchEntries);
      currentBatch = new ArrayList<>(batchEntries.size());

      for (AbstractBatchEntry batchEntry : batchEntries) {
        Point point = batchEntry.getPoint();
        currentBatch.add(point);
        if (batchEntry instanceof HttpBatchEntry) {
            HttpBatchEntry httpBatchEntry = HttpBatchEntry.class.cast(batchEntry);
            String dbName = httpBatchEntry.getDb();
            String rp = httpBatchEntry.getRp();
            String batchKey = dbName + "_" + rp;
            if (!batchKeyToBatchPoints.containsKey(batchKey)) {
              BatchPoints batchPoints = BatchPoints.database(dbName)
                                                   .retentionPolicy(rp).consistency(getConsistencyLevel()).build();
              batchKeyToBatchPoints.put(batchKey, batchPoints);
            }
            batchKeyToBatchPoints.get(batchKey).point(point);
        } else if (batchEntry instanceof UdpBatchEntry) {
            UdpBatchEntry udpBatchEntry = UdpBatchEntry.class.cast(batchEntry);
            int udpPort = udpBatchEntry.getUdpPort();
            if (!udpPortToBatchPoints.containsKey(udpPort)) {
              List<String> batchPoints = new ArrayList<String>();
              udpPortToBatchPoints.put(udpPort, batchPoints);
            }
            udpPortToBatchPoints.get(udpPort).add(point.lineProtocol());
        }
      }

      BatchProcessor.this.batchWriter.write(batchKeyToBatchPoints.values());

      for (Entry<Integer, List<String>> entry : udpPortToBatchPoints.entrySet()) {
          for (String lineprotocolStr : entry.getValue()) {
              BatchProcessor.this.influxDB.write(entry.getKey(), lineprotocolStr);
          }
      }
    } catch (Throwable t) {
      // any exception wouldn't stop the scheduler
      exceptionHandler.accept(currentBatch, t);
      LOG.log(Level.SEVERE, "Batch could not be sent. Data will be lost", t);
    }
  }

  /**
   * Put a single BatchEntry to the cache for later processing.
   *
   * @param batchEntry
   *            the batchEntry to write to the cache.
   */
  void put(final AbstractBatchEntry batchEntry) {
    try {
        this.queue.put(batchEntry);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
    if (this.queue.size() >= this.actions) {
      this.scheduler.submit(new Runnable() {
        @Override
        public void run() {
          write();
        }
      });
    }
  }

  /**
   * Flush the current open writes to influxdb and end stop the reaper thread. This should only be
   * called if no batch processing is needed anymore.
   *
   */
  void flushAndShutdown() {
    this.write();
    this.scheduler.shutdown();
    this.batchWriter.close();
  }

  /**
   * Flush the current open writes to InfluxDB. This will block until all pending points are written.
   */
  void flush() {
    this.write();
  }

  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

}

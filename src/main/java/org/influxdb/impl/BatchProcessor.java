package org.influxdb.impl;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Sets;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * A BatchProcessor can be attached to a InfluxDB Instance to collect single point writes and
 * aggregates them to BatchPoints to get a better write performance.
 *
 * @author stefan.majer [at] gmail.com
 */
public class BatchProcessor {
    protected final BlockingQueue<BatchEntry> queue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    final InfluxDBImpl influxDB;
    final int actions;
    private final TimeUnit flushIntervalUnit;
    private final int flushInterval;
    private final RecoverEntryCollection recoverEntryCollection;

    /**
     * The Builder to create a BatchProcessor instance.
     */
    public static final class Builder {
        private final InfluxDBImpl influxDB;
        private int actions;
        private int recoverLimit;
        private TimeUnit flushIntervalUnit;
        private int flushInterval;

        /**
         * @param influxDB is mandatory.
         */
        public Builder(final InfluxDB influxDB) {
            this.influxDB = (InfluxDBImpl) influxDB;
        }

        /**
         * The number of actions after which a batchwrite must be performed.
         *
         * @param maxActions number of Points written after which a write must happen.
         * @return this Builder to use it fluent
         */
        public Builder actions(final int maxActions) {
            this.actions = maxActions;
            return this;
        }

        /**
         * The max number of actions that will be recovered if influx fails to write the data.
         *
         * @param recoverLimit max number of Points stored after writes fail to happen.
         * @return this Builder to use it fluent
         */
        public Builder recover(final int recoverLimit) {
            this.recoverLimit = recoverLimit;
            return this;
        }

        /**
         * The interval at which at least should issued a write.
         *
         * @param interval the interval
         * @param unit     the TimeUnit of the interval
         * @return this Builder to use it fluent
         */
        public Builder interval(final int interval, final TimeUnit unit) {
            this.flushInterval = interval;
            this.flushIntervalUnit = unit;
            return this;
        }

        /**
         * Create the BatchProcessor.
         *
         * @return the BatchProcessor instance.
         */
        public BatchProcessor build() {
            Preconditions.checkNotNull(this.actions, "actions may not be null");
            Preconditions.checkNotNull(this.flushInterval, "flushInterval may not be null");
            Preconditions.checkNotNull(this.flushIntervalUnit, "flushIntervalUnit may not be null");
            return new BatchProcessor(this.influxDB, this.actions, this.recoverLimit, this.flushIntervalUnit, this.flushInterval);
        }
    }

    /**
     * Static method to create the Builder for this BatchProcessor.
     *
     * @param influxDB the influxdb database handle.
     * @return the Builder to create the BatchProcessor.
     */
    public static Builder builder(final InfluxDB influxDB) {
        return new Builder(influxDB);
    }

    BatchProcessor(final InfluxDBImpl influxDB, final int actions, final int recoverLimit,
                   final TimeUnit flushIntervalUnit, final int flushInterval) {
        super();
        this.influxDB = influxDB;
        this.actions = actions;
        this.recoverEntryCollection = recoverLimit > 0 ? new RecoverEntryCollection(recoverLimit) : null;
        this.flushIntervalUnit = flushIntervalUnit;
        this.flushInterval = flushInterval;

        // Flush at specified Rate
        this.scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    write();
                } catch (Exception ex) {
                    // TODO: Add logging so that this exception does not disappear
                }
            }
        }, this.flushInterval, this.flushInterval, this.flushIntervalUnit);

    }

    void write() {
        if (this.queue.isEmpty()) {
            return;
        }

        List<BatchEntry> batchEntries = new ArrayList<>(this.queue.size());
        this.queue.drainTo(batchEntries);

        write(batchEntries);
    }

    void write(final List<BatchEntry> batchEntries) {
        Map<String, BatchPoints> databaseToBatchPoints = Maps.newHashMap();
        for (BatchEntry batchEntry : batchEntries) {
            String dbName = batchEntry.getDb();
            if (!databaseToBatchPoints.containsKey(dbName)) {
                BatchPoints batchPoints = BatchPoints.database(dbName).retentionPolicy(batchEntry.getRp()).build();
                databaseToBatchPoints.put(dbName, batchPoints);
            }
            Point point = batchEntry.getPoint();
            databaseToBatchPoints.get(dbName).point(point);
        }

        if (recoverEntryCollection == null) {
            writePointsWithoutRecovery(databaseToBatchPoints);
        } else {
            writePointsWithRecovery(databaseToBatchPoints);
        }
    }

    private void writePointsWithoutRecovery(Map<String, BatchPoints> databaseToBatchPoints) {
        try {
            for (BatchPoints point : databaseToBatchPoints.values()) {
                BatchProcessor.this.influxDB.write(point);
            }
        } catch (Exception ex) {
            // TODO: Logging of the exception
        }
    }

    private void writePointsWithRecovery(Map<String, BatchPoints> databaseToBatchPoints) {
        Set<String> successSet = new HashSet(databaseToBatchPoints.keySet().size());
        boolean wasSuccessful = false;
        try {
            for (Map.Entry<String, BatchPoints> keyPair : databaseToBatchPoints.entrySet()) {
                BatchProcessor.this.influxDB.write(keyPair.getValue());
                successSet.add(keyPair.getKey());
            }

            wasSuccessful = true;
        } catch (Exception ex) {
            // An exception occurred, re-queue the batchEntries
            // TODO: Logging of the exception
            for (Map.Entry<String, BatchPoints> keyPair : databaseToBatchPoints.entrySet()) {
                if (successSet.contains(keyPair.getKey()))
                    continue;

                recoverEntryCollection.add(keyPair.getKey(), keyPair.getValue().getRetentionPolicy(), keyPair.getValue().getPoints());
            }
        }

        if (wasSuccessful) {
            List<BatchEntry> recoverEntries = recoverEntryCollection.drainEntries();
            if (recoverEntries != null && !recoverEntries.isEmpty())
                write(recoverEntries);
        }
    }

    /**
     * Put a single BatchEntry to the cache for later processing.
     *
     * @param batchEntry the batchEntry to write to the cache.
     */
    void put(final BatchEntry batchEntry) {
        this.queue.add(batchEntry);
        if (this.queue.size() >= this.actions) {
            write();
        }
    }

    /**
     * Flush the current open writes to influxdb and end stop the reaper thread. This should only be
     * called if no batch processing is needed anymore.
     */
    void flush() {
        this.write();
        this.scheduler.shutdown();
    }

}

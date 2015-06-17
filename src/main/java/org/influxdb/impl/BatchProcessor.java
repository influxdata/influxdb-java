package org.influxdb.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
 *
 */
public class BatchProcessor {
	protected final List<BatchEntry> cache = Collections.synchronizedList(new ArrayList<BatchEntry>());
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	final InfluxDBImpl influxDB;
	final int actions;
	private final TimeUnit flushIntervalUnit;
	private final int flushInterval;

	/**
	 * The Builder to create a BatchProcessor instance.
	 */
	public static class Builder {
		private final InfluxDBImpl influxDB;
		private int actions;
		private TimeUnit flushIntervalUnit;
		private int flushInterval;

		/**
		 * @param influxDB
		 *            is mandadory.
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
		 * The intervall at which at least should issued a write.
		 * 
		 * @param interval
		 *            the intervall
		 * @param unit
		 *            the TimeUnit of the intervaall
		 *
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
			return new BatchProcessor(this.influxDB, this.actions, this.flushIntervalUnit, this.flushInterval);
		}
	}

	static class BatchEntry {
		private final Point point;
		private final String db;
		private final String rp;

		public BatchEntry(final Point point, final String db, final String rp) {
			super();
			this.point = point;
			this.db = db;
			this.rp = rp;
		}

		public Point getPoint() {
			return this.point;
		}

		public String getDb() {
			return this.db;
		}

		public String getRp() {
			return this.rp;
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

	BatchProcessor(final InfluxDBImpl influxDB, final int actions, final TimeUnit flushIntervalUnit,
			final int flushInterval) {
		super();
		this.influxDB = influxDB;
		this.actions = actions;
		this.flushIntervalUnit = flushIntervalUnit;
		this.flushInterval = flushInterval;

		// Flush at specified Rate
		this.scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				write();
			}
		}, this.flushInterval, this.flushInterval, this.flushIntervalUnit);

	}

	void write() {
		if (this.cache.isEmpty()) {
			return;
		}
		Map<String, BatchPoints> databaseToBatchPoints = Maps.newHashMap();
		synchronized (this.cache) {
			for (BatchEntry batchEntry : this.cache) {
				String dbName = batchEntry.getDb();
				if (!databaseToBatchPoints.containsKey(dbName)) {
					BatchPoints batchPoints = new BatchPoints.Builder(dbName)
							.retentionPolicy(batchEntry.getRp())
							.build();
					databaseToBatchPoints.put(dbName, batchPoints);
				}
				Point point = batchEntry.getPoint();
				databaseToBatchPoints.get(dbName).point(point);
			}
			this.cache.clear();
		}
		for (BatchPoints batchPoints : databaseToBatchPoints.values()) {
			BatchProcessor.this.influxDB.write(batchPoints);
		}
	}

	private final AtomicLong issuedBatches = new AtomicLong();

	/**
	 * Put a single BatchEntry to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 */
	void put(final BatchEntry batchEntry) {
		if (this.issuedBatches.incrementAndGet() >= this.actions) {
			this.issuedBatches.set(0);
			write();
		}
		this.cache.add(batchEntry);
	}

	/**
	 * Flush the current open writes to influxdb and end stop the reaper thread. This should only be
	 * called if no batchprocessing is needed anymore.
	 *
	 */
	void flush() {
		this.write();
		this.scheduler.shutdown();
	}

}

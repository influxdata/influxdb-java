package org.influxdb.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.Point;

import com.google.common.base.Objects;
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
	protected final BlockingQueue<BatchEntry> queue = new LinkedBlockingQueue<>();
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	final InfluxDBImpl influxDB;
	final int actions;
	private final TimeUnit flushIntervalUnit;
	private final int flushInterval;

	/**
	 * The Builder to create a BatchProcessor instance.
	 */
	public static final class Builder {
		private final InfluxDBImpl influxDB;
		private int actions;
		private TimeUnit flushIntervalUnit;
		private int flushInterval;

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
		 * Create the BatchProcessor.
		 *
		 * @return the BatchProcessor instance.
		 */
		public BatchProcessor build() {
			Preconditions.checkNotNull(actions, "actions may not be null");
			Preconditions.checkNotNull(flushInterval, "flushInterval may not be null");
			Preconditions.checkNotNull(flushIntervalUnit, "flushIntervalUnit may not be null");
			return new BatchProcessor(influxDB, actions, flushIntervalUnit, flushInterval);
		}
	}

	static class BatchEntry {
		private final Point point;
		private final String database;
		private final String retentionPolicy;
		private final ConsistencyLevel consistencyLevel;

		public BatchEntry(final Point point, final String database, ConsistencyLevel consistencyLevel, final String retentionPolicy) {
			super();
			this.point = point;
			this.database = database;
			this.retentionPolicy = retentionPolicy;
			this.consistencyLevel = consistencyLevel;
		}

		public Point getPoint() {
			return point;
		}

		public String getDatabase() {
			return database;
		}

		public String getRetentionPolicy() {
			return retentionPolicy;
		}
		
		public ConsistencyLevel getConsistencyLevel() {
			return consistencyLevel;
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
		scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				write();
			}
		}, this.flushInterval, this.flushInterval, this.flushIntervalUnit);

	}

	void write() {
		if (queue.isEmpty()) {
			return;
		}

		Map<BatchCommonFields, ArrayList<Point>> databaseToBatchPoints = Maps.newHashMap();
		List<BatchEntry> batchEntries = new ArrayList<>(queue.size());
		queue.drainTo(batchEntries);

		for (BatchEntry batchEntry : batchEntries) {
			BatchCommonFields common = BatchCommonFields.fromEntry(batchEntry);
			
			if (!databaseToBatchPoints.containsKey(common)) {
				databaseToBatchPoints.put(common, new ArrayList<Point>());
			}
			databaseToBatchPoints.get(common).add(batchEntry.getPoint());
		}

		for (Entry<BatchCommonFields, ArrayList<Point>> entry : databaseToBatchPoints.entrySet()) {
			BatchCommonFields common = entry.getKey();
			List<Point> points = entry.getValue();
			influxDB.write(common.database, common.retentionPolicy, common.consistencyLevel, points);
		}
	}

	/**
	 * Put a single BatchEntry to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 */
	void put(final BatchEntry batchEntry) {
		queue.add(batchEntry);
		if (queue.size() >= actions) {
			write();
		}
	}

	/**
	 * Flush the current open writes to influxdb and end stop the reaper thread. This should only be
	 * called if no batch processing is needed anymore.
	 *
	 */
	void flush() {
		write();
		scheduler.shutdown();
	}

	private static class BatchCommonFields {
		private final String database;
		private final String retentionPolicy;
		private final ConsistencyLevel consistencyLevel;

		public BatchCommonFields(final String database, final String retentionPolicy,
				final ConsistencyLevel consistencyLevel) {
			this.database = database;
			this.retentionPolicy = retentionPolicy;
			this.consistencyLevel = consistencyLevel;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(database, retentionPolicy, consistencyLevel);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			BatchCommonFields other = (BatchCommonFields) obj;

			return (Objects.equal(database, other.database)
					&& Objects.equal(retentionPolicy, other.retentionPolicy)
					&& Objects.equal(consistencyLevel, other.consistencyLevel));
		}
		
		public static BatchCommonFields fromEntry(BatchEntry batchEntry) {
			return new BatchCommonFields(batchEntry.getDatabase(),
					batchEntry.getRetentionPolicy(), batchEntry.getConsistencyLevel());
		}


	}
}

package org.influxdb.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * A BatchProcessor can be attached to a InfluxDB Instance to collect single point writes and
 * aggregates them to BatchPoints to get a better write performance.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class BatchProcessor {
   private static final Logger LOGGER = LoggerFactory.getLogger(BatchProcessor.class);

	protected final BlockingQueue<BatchEntry> queue = new LinkedBlockingQueue<>();
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   private final InfluxDBImpl influxDB;
   private final int actions;
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
		public Builder(InfluxDB influxDB) {
			this.influxDB = (InfluxDBImpl) influxDB;
		}

		/**
		 * The number of actions after which a batchwrite must be performed.
		 *
		 * @param maxActions
		 *            number of Points written after which a write must happen.
		 * @return this Builder to use it fluent
		 */
		public Builder actions(int maxActions) {
         actions = maxActions;
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
		public Builder interval(int interval, TimeUnit unit) {
         flushInterval = interval;
         flushIntervalUnit = unit;
			return this;
		}

		/**
		 * Create the BatchProcessor.
		 *
		 * @return the BatchProcessor instance.
		 */
		public BatchProcessor build() {
         LOGGER.debug("Actions {}, flushInterval [unit] {} [{}]", actions, flushInterval, flushIntervalUnit);
			Preconditions.checkNotNull(actions, "actions may not be null");
			Preconditions.checkNotNull(flushInterval, "flushInterval may not be null");
			Preconditions.checkNotNull(flushIntervalUnit, "flushIntervalUnit may not be null");
			return new BatchProcessor(influxDB, actions, flushIntervalUnit, flushInterval);
		}
	}

	static class BatchEntry {
		private final Point point;
		private final String db;
		private final String rp;

		public BatchEntry(Point point, String db, String rp) {
			this.point = point;
			this.db = db;
			this.rp = rp;
		}

		public Point getPoint() {
			return point;
		}

		public String getDb() {
			return db;
		}

		public String getRp() {
			return rp;
		}
	}

	/**
	 * Static method to create the Builder for this BatchProcessor.
	 *
	 * @param influxDB
	 *            the influxdb database handle.
	 * @return the Builder to create the BatchProcessor.
	 */
	public static Builder builder(InfluxDB influxDB) {
		return new Builder(influxDB);
	}

	BatchProcessor(InfluxDBImpl influxDB, int actions, TimeUnit flushIntervalUnit, int flushInterval) {
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

		Map<String, BatchPoints> databaseToBatchPoints = Maps.newHashMap();
		List<BatchEntry> batchEntries = new ArrayList<>(queue.size());
      queue.drainTo(batchEntries);

		for (BatchEntry batchEntry : batchEntries) {
			String dbName = batchEntry.getDb();
			if (!databaseToBatchPoints.containsKey(dbName)) {
				BatchPoints batchPoints = BatchPoints.database(dbName).retentionPolicy(batchEntry.getRp()).build();
				databaseToBatchPoints.put(dbName, batchPoints);
			}
			Point point = batchEntry.getPoint();
			databaseToBatchPoints.get(dbName).point(point);
		}

		for (BatchPoints batchPoints : databaseToBatchPoints.values()) {
         influxDB.write(batchPoints);
		}
	}

	/**
	 * Put a single BatchEntry to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 */
	void put(BatchEntry batchEntry) {
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

}

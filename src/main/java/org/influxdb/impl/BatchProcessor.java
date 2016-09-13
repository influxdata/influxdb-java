package org.influxdb.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;

/**
 * A BatchProcessor can be attached to a InfluxDB Instance to collect single point writes and
 * aggregates them to BatchPoints to get a better write performance.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class BatchProcessor {

	private static final Logger logger = Logger.getLogger(BatchProcessor.class.getName());
	protected final BlockingQueue<BatchEntry> queue = new LinkedBlockingQueue<>();
	protected final BlockingQueue<BatchEntryLineProtocol> queuelineprotocol = new LinkedBlockingQueue<>();
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

	static class BatchEntryLineProtocol {
		private final String cmdLine;
		private final String db;
		private final String rp;
		private final ConsistencyLevel cLvl;

		public BatchEntryLineProtocol(final String cmdLine, final String db, final String rp, final ConsistencyLevel cLvl) {
			super();
			this.cmdLine = cmdLine;
			this.db = db;
			this.rp = rp;
			this.cLvl = cLvl;
		}

		public String getcmdLine() {
			return this.cmdLine;
		}

		public String getDb() {
			return this.db;
		}

		public String getRp() {
			return this.rp;
		}

		public ConsistencyLevel getcLvl() {
			return this.cLvl;
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
		
		if (!this.queue.isEmpty()) {
			writePoints();
		}
		
		if (!this.queuelineprotocol.isEmpty()) {
			writeLineProtocol();
		}		
	}
	
	void writePoints() {
		try {
			if (this.queue.isEmpty()) {
				return;
			}

			Map<String, BatchPoints> databaseToBatchPoints = Maps.newHashMap();
			List<BatchEntry> batchEntries = new ArrayList<>(this.queue.size());
			this.queue.drainTo(batchEntries);

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
				BatchProcessor.this.influxDB.write(batchPoints);
			}
		} catch (Throwable t) {
			// any exception would stop the scheduler
			logger.log(Level.SEVERE, "Batch could not be sent. Data will be lost", t);
		}
	}

	void writeLineProtocol() {
		try {

			if (this.queuelineprotocol.isEmpty()) {
				return;
			}
					
			HashMultimap<String, BatchEntryLineProtocol> databaseToBatchPoints = HashMultimap.create();
			List<BatchEntryLineProtocol> batchEntries = new ArrayList<>(this.queuelineprotocol.size());
			this.queuelineprotocol.drainTo(batchEntries);

			for (BatchEntryLineProtocol batchEntry : batchEntries) {
				String sKey = batchEntry.getDb() + "." + batchEntry.getRp();							
				databaseToBatchPoints.put(sKey, batchEntry);							
			}
						
			for (String sKey : databaseToBatchPoints.keySet()) {
				List<String> records = new ArrayList<>();				
				BatchEntryLineProtocol bLineEntry = null;

				Set<BatchEntryLineProtocol> bEntries = databaseToBatchPoints.get(sKey);
								
				for (BatchEntryLineProtocol bEntry : bEntries) {

					if (bLineEntry == null)
						bLineEntry = bEntry;

					records.add(bEntry.getcmdLine());									
				}				
				
				BatchProcessor.this.influxDB.write(bLineEntry.getDb(), bLineEntry.getRp(), bLineEntry.getcLvl(), records);				
			}

		} catch (Throwable t) {
			// any exception would stop the scheduler
			logger.log(Level.SEVERE, "Batch could not be sent. Data will be lost", t);
		}
	}
	
	/**
	 * Put a single BatchEntry to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 */
	void put(final BatchEntry batchEntry) {
		this.queue.add(batchEntry);
		if (this.queue.size() >= this.actions) {
			writePoints();
		}
	}
	
	/**
	 * Put a single BatchEntryLineProtocol to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 */
	void put(final BatchEntryLineProtocol batchEntry) {
		this.queuelineprotocol.add(batchEntry);
		if (this.queuelineprotocol.size() >= this.actions) {
			writeLineProtocol();
		}
	}

	/**
	 * Flush the current open writes to influxdb and end stop the reaper thread. This should only be
	 * called if no batch processing is needed anymore.
	 *
	 */
	void flush() {
		this.write();		
		this.scheduler.shutdown();
	}

}

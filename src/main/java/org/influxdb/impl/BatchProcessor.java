package org.influxdb.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.influxdb.InfluxDB;
import org.influxdb.dto.AbstractBatchPoints;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.UdpBatchPoints;

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

	private static final Logger logger = Logger.getLogger(BatchProcessor.class.getName());
	protected final BlockingQueue<AbstractBatchEntry> queue = new LinkedBlockingQueue<>();
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
	
	static class AbstractBatchEntry {
		private final Point point;

		public AbstractBatchEntry(final Point point) {
			this.point = point;
		}

		public Point getPoint() {
			return this.point;
		}
	}

	static class BatchEntry extends AbstractBatchEntry{
		private final String db;
		private final String rp;

		public BatchEntry(final Point point, final String db, final String rp) {
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
	
	static class UdpBatchEntry extends AbstractBatchEntry{
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
		try {
			if (this.queue.isEmpty()) {
				return;
			}

			Map<String, BatchPoints> databaseToBatchPoints = Maps.newHashMap();
			Map<Integer, UdpBatchPoints> udpPortToBatchPoints = Maps.newHashMap();

			List<AbstractBatchEntry> abstractBatchEntries = new ArrayList<>(this.queue.size());
			this.queue.drainTo(abstractBatchEntries);

			assignBatchEntriesToMap(abstractBatchEntries, databaseToBatchPoints, udpPortToBatchPoints);

			write(databaseToBatchPoints, udpPortToBatchPoints);
		} catch (Throwable t) {
			// any exception wouldn't stop the scheduler
			logger.log(Level.SEVERE, "Batch could not be sent. Data will be lost", t);
		}
	}

	private void write(Map<String, BatchPoints> databaseToBatchPoints,
			Map<Integer, UdpBatchPoints> udpPortToBatchPoints) {
		for (BatchPoints batchPoints : databaseToBatchPoints.values()) {
			BatchProcessor.this.influxDB.write(batchPoints);
		}
		
		for (UdpBatchPoints udpBatchPoints : udpPortToBatchPoints.values()) {
			BatchProcessor.this.influxDB.write(udpBatchPoints);
		}
	}

	private void assignBatchEntriesToMap(List<AbstractBatchEntry> abstractBatchEntries,
			Map<String, BatchPoints> databaseToBatchPoints, Map<Integer, UdpBatchPoints> udpToBatchPoints) {
		for(AbstractBatchEntry abstractBatchEntry: abstractBatchEntries){
			AbstractBatchPoints<?> abstractBatchPoints = getExistedOrCreateBatchPoints(abstractBatchEntry, databaseToBatchPoints, udpToBatchPoints);
			abstractBatchPoints.point(abstractBatchEntry.getPoint());
		}
	}
	
	private AbstractBatchPoints<?> getExistedOrCreateBatchPoints(AbstractBatchEntry abstractBatchEntry,
			Map<String, BatchPoints> databaseToBatchPoints, Map<Integer, UdpBatchPoints> udpToBatchPoints) {
		if (abstractBatchEntry instanceof BatchEntry) {
			final BatchEntry batchEntry = (BatchEntry) abstractBatchEntry;
			String dbName = batchEntry.getDb();
			BatchPoints computeIfAbsent = databaseToBatchPoints.computeIfAbsent(dbName,
				new Function<String, BatchPoints>() {
					@Override
					public BatchPoints apply(String dbName) {
						return BatchPoints.database(dbName).retentionPolicy(batchEntry.getRp()).build();
					}
				});
			return computeIfAbsent;

		}

		if (abstractBatchEntry instanceof UdpBatchEntry) {
			final UdpBatchEntry batchEntry = (UdpBatchEntry) abstractBatchEntry;
			int udpPort = batchEntry.getUdpPort();
			return udpToBatchPoints.computeIfAbsent(udpPort, new Function<Integer, UdpBatchPoints>() {
				@Override
				public UdpBatchPoints apply(Integer udpPort) {
					return UdpBatchPoints.udpPort(udpPort).build();
				}
			});

		}

		throw new RuntimeException("BatchEntry instance isn't belong to UdpBatchEntry or BatchEntry");
	}
	

	/**
	 * Put a single BatchEntry to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 */
	void put(final AbstractBatchEntry batchEntry) {
		this.queue.add(batchEntry);
		if (this.queue.size() >= this.actions) {
			write();
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

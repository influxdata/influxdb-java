package org.influxdb.impl;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.Point;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

/**
 * A BatchProcessor can be attached to a InfluxDB Instance to collect single
 * point writes and aggregates them to BatchPoints to get a better write
 * performance.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class BatchProcessor {
	protected final Queue<BatchEntry> queue;
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	final InfluxDBImpl influxDB;
	final int actions;
	private final TimeUnit flushIntervalUnit;
	private final int flushInterval;
	private BufferFailBehaviour behaviour;

	/**
	 * The Builder to create a BatchProcessor instance.
	 */
	public static final class Builder {
		private final InfluxDBImpl influxDB;
		private int actions;
		private TimeUnit flushIntervalUnit;
		private int flushInterval;
		private int capacity = 0;
		private BufferFailBehaviour behaviour = BufferFailBehaviour.THROW_EXCEPTION;

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
		 *            number of Points written after which a write should
		 *            happen.
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
		 * The maximum queue capacity.
		 * 
		 * @param capacity
		 *            the maximum number of points to hold
		 * @return this {@code Builder}, to allow chaining
		 */
		public Builder capacity(final int capacity) {
			this.capacity = capacity;
			return this;
		}

		/**
		 * The behaviour when a put to the buffer fails
		 * 
		 * @param behaviour
		 * @return
		 */
		public Builder behaviour(final BufferFailBehaviour behaviour) {
			this.behaviour = behaviour;
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

			if (capacity < 1) {
				Preconditions.checkArgument(behaviour != BufferFailBehaviour.DROP_OLDEST,
						"Behaviour cannot be DROP_OLDEST if depth not set");
			}

			return new BatchProcessor(this.influxDB, this.actions, this.flushIntervalUnit, this.flushInterval,
					this.capacity, this.behaviour);
		}
	}

	static class BatchEntry {
		private final Point point;
		private final String database;
		private final String retentionPolicy;
		private final ConsistencyLevel consistency;

		public BatchEntry(final Point point, final String database, final String retentionPolicy,
				final ConsistencyLevel consistency) {
			this.point = point;
			this.database = database;
			this.retentionPolicy = retentionPolicy;
			this.consistency = consistency;
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
			return consistency;
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

	enum BufferFailBehaviour {
		THROW_EXCEPTION, DROP_CURRENT, DROP_OLDEST,
	}

	BatchProcessor(final InfluxDBImpl influxDB, final int actions, final TimeUnit flushIntervalUnit,
			final int flushInterval, final int capacity, final BufferFailBehaviour behaviour) {
		super();
		this.influxDB = influxDB;
		this.actions = actions;
		this.flushIntervalUnit = flushIntervalUnit;
		this.flushInterval = flushInterval;
		this.behaviour = behaviour;

		if (capacity > 0) {
			if (behaviour == BufferFailBehaviour.DROP_OLDEST) {
				EvictingQueue<BatchEntry> evictingQueue = EvictingQueue.create(capacity);
				queue = Queues.synchronizedQueue(evictingQueue);
			} else {
				queue = Queues.newLinkedBlockingQueue(capacity);
			}
		} else {
			queue = Queues.newLinkedBlockingQueue();
		}

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

		// TODO: Review whether Multimap is the most suitable class here
		// Could also use Map<BatchCommonFields, List<Point>>
		final ArrayListMultimap<BatchCommonFields, BatchEntry> commonFieldsToEntiesMap = ArrayListMultimap.create();
		queue.forEach(new Consumer<BatchEntry>() {
			@Override
			public void accept(BatchEntry batchEntry) {
				BatchCommonFields common = new BatchCommonFields(batchEntry.getDatabase(),
						batchEntry.getRetentionPolicy(), batchEntry.getConsistencyLevel());
				commonFieldsToEntiesMap.get(common).add(batchEntry);
			}
		});

		for (BatchCommonFields common : commonFieldsToEntiesMap.keySet()) {
			List<BatchEntry> batchEntries = commonFieldsToEntiesMap.get(common);
			List<Point> points = Lists.transform(batchEntries, new Function<BatchEntry, Point>() {
				@Override
				public Point apply(BatchEntry input) {
					return input.point;
				}
			});
			influxDB.writeBatched(common.database, common.retentionPolicy, common.consistencyLevel, points);
			// If the points were written, remove them from the queue
			// TODO: Potentially here we should implement policy about keeping points that failed 
			// their 'write'. I.e. we could catch exceptions from writeBatched() and remove the points
			// based on the failure
			queue.removeAll(points);
		}
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
	}

	/**
	 * Put a single BatchEntry to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 * @return
	 */
	public boolean put(String database, String retentionPolicy, ConsistencyLevel consistency, Point point) {
		BatchEntry entry = new BatchEntry(point, database, retentionPolicy, consistency);
		boolean added;

		if (behaviour == BufferFailBehaviour.THROW_EXCEPTION) {
			added = queue.add(entry);
		} else {
			added = queue.offer(entry);
		}

		if (queue.size() >= actions) {
			write();
		}

		return added;
	}

	/**
	 * Flush the current open writes to influxdb and end stop the reaper thread.
	 * This should only be called if no batch processing is needed anymore.
	 *
	 */
	void flush() {
		write();
		scheduler.shutdown();
	}

}

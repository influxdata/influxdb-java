package org.influxdb.impl;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
	public static final int DEFAULT_ACTIONS = 10;
	public static final int DEFAULT_FLUSH_INTERVAL = 100;
	public static final TimeUnit DEFAULT_FLUSH_INTERVAL_TIME_UINT = TimeUnit.MILLISECONDS;
	
	protected final Queue<BatchEntry> queue;
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	final InfluxDBImpl influxDB;
	final int actions;
	private final TimeUnit flushIntervalUnit;
	private final int flushInterval;
	private BufferFailBehaviour behaviour;
	private boolean discardOnFailedWrite = true;
	private AtomicBoolean writeLockout = new AtomicBoolean(false);

	/**
	 * The Builder to create a BatchProcessor instance.
	 */
	public static final class Builder {
		private final InfluxDBImpl influxDB;
		private int actions = DEFAULT_ACTIONS;
		private TimeUnit flushIntervalUnit = DEFAULT_FLUSH_INTERVAL_TIME_UINT;
		private int flushInterval = DEFAULT_FLUSH_INTERVAL;
		private Integer capacity = null;
		private BufferFailBehaviour behaviour = BufferFailBehaviour.THROW_EXCEPTION;
		private boolean discardOnFailedWrite = true;

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
		 *            the maximum number of points to hold Should be NULL, for
		 *            no buffering OR > 0 for buffering (NB: a capacity of 1
		 *            will not really buffer)
		 * @return this {@code Builder}, to allow chaining
		 */
		public Builder capacity(final Integer capacity) {
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
		 * Controls whether the buffer will keep or discard buffered points on
		 * network errors.
		 * 
		 * @param discardOnFailedWrite
		 * @return
		 */
		public Builder discardOnFailedWrite(final boolean discardOnFailedWrite) {
			this.discardOnFailedWrite = discardOnFailedWrite;
			return this;
		}

		/**
		 * Create the BatchProcessor.
		 *
		 * @return the BatchProcessor instance.
		 */
		public BatchProcessor build() {
			Preconditions.checkArgument(actions > 0, "actions must be > 0");
			Preconditions.checkArgument(flushInterval > 0, "flushInterval must be > 0");
			Preconditions.checkNotNull(flushIntervalUnit, "flushIntervalUnit may not be null");

			if (capacity != null) {
				Preconditions.checkArgument(capacity > 0,
						"Capacity should be > 0 or NULL");
				Preconditions.checkArgument(capacity > actions,
						"Capacity must be > than actions");
			} else {
				Preconditions.checkArgument(behaviour != BufferFailBehaviour.DROP_OLDEST,
						"Behaviour cannot be DROP_OLDEST if capacity not set");
			}

			return new BatchProcessor(influxDB, actions, flushIntervalUnit, flushInterval,
					capacity, behaviour, discardOnFailedWrite);
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
			final int flushInterval, final Integer capacity, final BufferFailBehaviour behaviour, boolean discardOnFailedWrite) {
		super();
		this.influxDB = influxDB;
		this.actions = actions;
		this.flushIntervalUnit = flushIntervalUnit;
		this.flushInterval = flushInterval;
		this.behaviour = behaviour;
		this.discardOnFailedWrite = discardOnFailedWrite;

		if (capacity != null) {
			if (capacity == 0) {
				throw new IllegalArgumentException("capacity cannot be 0");
			}
			
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
				writeLockout.set(false);;
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
			
			try {
				influxDB.writeBatched(common.database, common.retentionPolicy, common.consistencyLevel, points);
				// If the points were written, remove them from the queue
				queue.removeAll(batchEntries);
			} catch (Exception e) {
				if (discardOnFailedWrite) {
					queue.removeAll(batchEntries);
				} else {
					writeLockout.set(true);;
				}
			}
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

		if ((!writeLockout.get()) && (queue.size() >= actions)) {
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

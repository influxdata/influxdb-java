package org.influxdb.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.BufferFailBehaviour;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A BatchProcessor can be attached to a InfluxDB Instance to collect single point writes and
 * aggregates them to BatchPoints to get a better write performance.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class BatchProcessor {
	private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);
	public static final int DEFAULT_ACTIONS = 10;
	public static final int DEFAULT_FLUSH_INTERVAL_MIN = 1000;
	public static final int DEFAULT_FLUSH_INTERVAL_MAX = 60000;
	public static final TimeUnit DEFAULT_FLUSH_INTERVAL_TIME_UINT = TimeUnit.MILLISECONDS;
	public static final int DEFAULT_MAX_BATCH_WRITE_SIZE = 50;

	private static final int BACKOFF_EXPONENT = 2;
	
	protected final BlockingDeque<BatchEntry> queue;
	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private final InfluxDBImpl influxDB;
	private final int flushActions;
	private final TimeUnit flushIntervalUnit;
	private int flushInterval;
	private final int flushIntervalMin;
	private final int flushIntervalMax;
	private final BufferFailBehaviour behaviour;
	private final boolean discardOnFailedWrite;
	private final int maxBatchWriteSize;

	private final AtomicBoolean writeInProgressLock = new AtomicBoolean(false);
	private final AtomicBoolean waitForFlushIntervalToWriteLock = new AtomicBoolean(false);

	private final Object queueLock = new Object();
	private final ArrayList<BatchEntry> writeList;
	

	/**
	 * The Builder to create a BatchProcessor instance.
	 */
	public static final class Builder {
		private final InfluxDBImpl influxDB;
		private int flushActions = DEFAULT_ACTIONS;
		private TimeUnit flushIntervalUnit = DEFAULT_FLUSH_INTERVAL_TIME_UINT;
		private int flushIntervalMin = DEFAULT_FLUSH_INTERVAL_MIN;
		private int flushIntervalMax = DEFAULT_FLUSH_INTERVAL_MAX;
		private Integer capacity = null;
		private BufferFailBehaviour behaviour = BufferFailBehaviour.THROW_EXCEPTION;
		private boolean discardOnFailedWrite = true;
		private int maxBatchWriteSize = DEFAULT_MAX_BATCH_WRITE_SIZE;

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
		 * @param actions
		 *            number of Points written after which a write should
		 *            happen.
		 * @return this Builder to use it fluent
		 */
		public Builder actions(final int flushActions) {
			this.flushActions = flushActions;
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
		public Builder interval(final int intervalMin, final int intervalMax, final TimeUnit unit) {
			this.flushIntervalMin = intervalMin;
			this.flushIntervalMax = intervalMax;
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
		 * Set both the capacity and actions
		 * @param capacity
		 * @param actions
		 * @return this builder instance, for fluent usage
		 */
		public Builder capacityAndActions(final Integer capacity, final int flushActions) {
			this.capacity = capacity;
			this.flushActions = flushActions;
			return this;
		}

		/**
		 * The behaviour when a put to the buffer fails
		 * 
		 * @param behaviour
		 * @return this builder instance, for fluent usage
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
		 * @return this builder instance, for fluent usage
		 */
		public Builder discardOnFailedWrite(final boolean discardOnFailedWrite) {
			this.discardOnFailedWrite = discardOnFailedWrite;
			return this;
		}
		
		/**
		 * The maximum number of points to write in a batch
		 * 
		 * @param maxBatchWriteSize
		 * @return this builder instance, for fluent usage
		 */
		public Builder maxBatchWriteSize(final int maxBatchWriteSize) {
			this.maxBatchWriteSize = maxBatchWriteSize;
			return this;
		}

		/**
		 * Create the BatchProcessor.
		 *
		 * @return the BatchProcessor instance.
		 */
		public BatchProcessor build() {
			Preconditions.checkArgument(flushActions > 0, "flushActions must be > 0");
			Preconditions.checkArgument(flushIntervalMin > 0, "flushIntervalMin must be > 0");
			Preconditions.checkNotNull(flushIntervalUnit, "flushIntervalUnit may not be null");
			Preconditions.checkArgument(flushIntervalMin <= flushIntervalMax, "flushIntervalMin must be <= flushIntervalMax");
			Preconditions.checkArgument(maxBatchWriteSize > 0, "maxBatchWriteSize must be > 0");
			
			if (capacity != null) {
				Preconditions.checkArgument(capacity > 0, "Capacity should be > 0 or NULL");
				Preconditions.checkArgument(capacity >= flushActions, "Capacity must be >= than flushActions");
			} else {
				Preconditions.checkArgument(behaviour != BufferFailBehaviour.DROP_OLDEST,
						"Behaviour cannot be DROP_OLDEST if capacity not set");
			}

			return new BatchProcessor(influxDB, flushActions, flushIntervalUnit, flushIntervalMin, flushIntervalMax,
					capacity, behaviour,
					discardOnFailedWrite, maxBatchWriteSize);
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

	BatchProcessor(final InfluxDBImpl influxDB, final int flushActions, final TimeUnit flushIntervalUnit,
			final int flushIntervalMin, final int flushIntervalMax, final Integer capacity, final BufferFailBehaviour behaviour,
			boolean discardOnFailedWrite, final int maxBatchWriteSize) {
		super();
		this.influxDB = influxDB;
		this.flushActions = flushActions;
		this.flushIntervalUnit = flushIntervalUnit;
		this.flushIntervalMin = flushIntervalMin;
		this.flushIntervalMax = flushIntervalMax;
		this.behaviour = behaviour;
		this.discardOnFailedWrite = discardOnFailedWrite;
		this.maxBatchWriteSize = maxBatchWriteSize;
		writeList = Lists.newArrayListWithCapacity(maxBatchWriteSize);
		
		flushInterval = this.flushIntervalMin;
		
		if (capacity != null) {
			if (capacity == 0) {
				throw new IllegalArgumentException("capacity cannot be 0");
			}
			queue = new LinkedBlockingDeque<BatchProcessor.BatchEntry>(capacity);
		} else {
			queue = new LinkedBlockingDeque<BatchEntry>();
		}
		
		// Flush at specified Rate
		scheduleNextFlush();
	}
	
	private void scheduleNextFlush() {
		logger.debug("scheduling next flush for {} {}", flushInterval, flushIntervalUnit);
		scheduler.schedule(new FlushIntervalRunnable(), flushInterval, flushIntervalUnit);
	}
	
	private class FlushIntervalRunnable implements Runnable {
		public void run() {
			logger.debug("Flush interval commenced");
			WriteResult result = attemptWrite();
			
			switch (result){
			case FAILED:
				logger.debug("Flush interval - FAILED");
				flushInterval = Math.min(flushInterval * BACKOFF_EXPONENT, flushIntervalMax);
				break;
			case NOT_ATTEMPTED:
				logger.debug("Flush interval - NOT ATTEMPTED");
				break;
			case SUCCESSFUL:
				logger.debug("Flush interval - SUCCESS");
				flushInterval = flushIntervalMin;
				waitForFlushIntervalToWriteLock.set(false);
				break;
			default:
				throw new RuntimeException("Unhandled WriteResult enum value:" + result);
			}
			
			scheduleNextFlush();
		}
	}
	
	private class WriteRunnable implements Runnable{
		@Override
		public void run() {
			attemptWrite();
		}
	}
	
	enum WriteResult {
		NOT_ATTEMPTED,
		SUCCESSFUL,
		FAILED,
	}
	
	WriteResult attemptWrite() {
		if (writeInProgressLock.compareAndSet(false, true)) {
			logger.debug("Attempting to write");
			boolean success = write();
			writeInProgressLock.set(false);
			
			return success ? WriteResult.SUCCESSFUL: WriteResult.FAILED;
		}

		logger.debug("Write already in progress, not attempting");
		return WriteResult.NOT_ATTEMPTED;
	}
	
	void writeNow() {
		// If there is no write in progress, schedule an immediate write
		if (!writeInProgressLock.get()) {
			logger.debug("Write NOT already in progress, scheduling WriteRunnable");
			scheduler.execute(new WriteRunnable());
		}
	}

	boolean write() {
		if (queue.isEmpty()) {
			return true;
		}

		synchronized (queueLock) {
			writeList.clear(); // probably redundant
			// Never write the whole queue, it could be very big, so just get a
			// temporary list
			queue.drainTo(writeList, maxBatchWriteSize);
		}

		// Map the writeList by the common (and hence batchable) fields
		Map<BatchCommonFields, ArrayList<BatchEntry>> databaseToBatchPoints = Maps.newHashMap();

		for (BatchEntry batchEntry : writeList) {
			BatchCommonFields common = BatchCommonFields.fromEntry(batchEntry);

			if (!databaseToBatchPoints.containsKey(common)) {
				databaseToBatchPoints.put(common, new ArrayList<BatchEntry>());
			}
			databaseToBatchPoints.get(common).add(batchEntry);
		}

		// For each collection of batchable fields, attempt a batched write
		for (Entry<BatchCommonFields, ArrayList<BatchEntry>> entry : databaseToBatchPoints.entrySet()) {
			BatchCommonFields common = entry.getKey();
			List<BatchEntry> batchEntries = entry.getValue();

			List<Point> points = Lists.transform(batchEntries, new Function<BatchEntry, Point>() {
						@Override
						public Point apply(BatchEntry input) {
							return input.point;
						}
					});

			try {
				influxDB.writeBatched(common.database, common.retentionPolicy, common.consistencyLevel, points);
				writeList.removeAll(batchEntries);
			} catch (Exception e) {
				// TODO: we should probably include some logging here
			}
		}

		if (!writeList.isEmpty()) {
			// Some points were not written, return them to the queue if
			// necessary
			synchronized (queueLock) {
				if (!discardOnFailedWrite) {
					// If we failed our write, add back the elements from this
					// attempt in REVERSE order to maintain queue ordering
					for (BatchEntry batchEntry : Lists.reverse(writeList)) {
						boolean insertedAtStart = queue.offerFirst(batchEntry);
						if (!insertedAtStart) {
							// We have inserted as much as we can, may as well
							// stop.

							// NB: There is possibly a need for an enhancement
							// here based on the behaviour attribute, but for
							// now I cannot think of a more reasonable action
							// than the current behaviour
							break;
						}
					}
					waitForFlushIntervalToWriteLock.set(true);
				}
				writeList.clear();
			}
			return false;
		}
		
		return true;
	}

	/**
	 * Put a single BatchEntry to the cache for later processing.
	 *
	 * @param batchEntry
	 *            the batchEntry to write to the cache.
	 * @return
	 */
	public boolean put(String database, String retentionPolicy, ConsistencyLevel consistency, Point point) {
		BatchEntry entry = new BatchEntry(point, database, consistency, retentionPolicy);
		boolean added = false;

		switch (behaviour) {
		case DROP_CURRENT:
			added = queue.offer(entry);
			break;
		case DROP_OLDEST:
			added = addAndDropIfNecessary(entry);
			break;
		case THROW_EXCEPTION:
			added = queue.add(entry);
			break;
		case BLOCK_THREAD:
			try {
				queue.put(entry);
				added = true;
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			break;
		default:
		throw new UnsupportedOperationException("Behaviour not yet supported");
		}

		logger.debug("Queue size:{}", queue.size());

		if (!waitForFlushIntervalToWriteLock.get()) {
			if (queue.size() >= flushActions) {
				logger.debug("No flush lock - Queue size[{}] actions[{}]", queue.size(), flushActions);
				writeNow();
			}
		}

		return added;
	}

	private boolean addAndDropIfNecessary(BatchEntry entry) {
		synchronized (queueLock) {
			boolean added = queue.offer(entry);
			if (!added) {
				queue.poll(); // Remove the front of the queue
				added = queue.add(entry);
			}
			return added;			
		}
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

	public int getBufferedCount() {
		synchronized (queueLock) {
			return writeList.size() + queue.size();
		}
	}

	/**
	 * Retrieves, but does not remove, the first element of the buffer
	 * 
	 * @return an Optional<Point> containing the first element in the queue
	 */
	public Optional<Point> peekFirstBuffered() {
		BatchEntry batchEntry = null;
		synchronized (queueLock) {
			if (!writeList.isEmpty()) {
				batchEntry = writeList.get(0);
			} else {
				batchEntry = queue.peekFirst();
			}
		}

		if (batchEntry == null) {
			return Optional.absent();
		}

		return Optional.of(batchEntry.point);
	}
}

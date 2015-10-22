package org.influxdb.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB.BufferFailBehaviour;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.Point;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BatchProcessorTest {
	private static class AnonInfluxDBImpl extends InfluxDBImpl {
		private static final String FAIL_DATABASE = "fail_db"; 
		private final boolean throwErrorOnWriteBatched;
		private int writeCalled = 0;
		
		public AnonInfluxDBImpl(final boolean throwErrorOnWriteBatched) {
			super("temp", "user", "pass");
			this.throwErrorOnWriteBatched = throwErrorOnWriteBatched;
		}
		
		@Override
		protected void writeBatched(String database, String retentionPolicy, ConsistencyLevel consistencyLevel,
				List<Point> points) {
			writeCalled++;
			if (throwErrorOnWriteBatched) {
				throw new RuntimeException("Anon error");
			}
			
			if (FAIL_DATABASE.equals(database)) {
				throw new RuntimeException("Will not write to fail db");
			}
		}
		
		@Override
		protected void writeUnbatched(String database, String retentionPolicy, ConsistencyLevel consistencyLevel,
				Point point) {
		}
	}
	
	private static class QueueDepthRecordingDBImpl extends InfluxDBImpl {
		private int queueDepth = 0;
		private int writeCalled = 0;

		public QueueDepthRecordingDBImpl() {
			super("temp", "user", "pass");
		}
		
		@Override
		protected void writeBatched(String database, String retentionPolicy,
				ConsistencyLevel consistencyLevel, List<Point> points) {
			writeCalled++;
			queueDepth = getBufferedCount();
		}
		
		public int getQueueDepth() {
			return queueDepth;
		}

		public BatchProcessor getBatchProcessor() {
			return super.getBatchProcessor();
		}
	}

	private static class NonScheduledWriteBatchProcessor extends BatchProcessor {

		NonScheduledWriteBatchProcessor(InfluxDBImpl influxDB, int actions, TimeUnit flushIntervalUnit,
				int flushInterval, Integer capacity, BufferFailBehaviour behaviour, boolean discardOnFailedWrite,
				int maxBatchWriteSize) {
			super(influxDB, actions, flushIntervalUnit, flushInterval, 5*flushInterval, capacity, behaviour, discardOnFailedWrite,
					maxBatchWriteSize);
		}
		@Override
		void writeNow() {
			attemptWrite();
		}
	}
	private static Point getAnonPoint() {
		return getPoint("anon");
	}
	
	private static Point getPoint(String measurement) {
		return Point.measurement(measurement)
    			.field("field", "value").build();
	}
	
	private static AnonInfluxDBImpl getAnonInfluxDB() {
		return new AnonInfluxDBImpl(false);
	}
	
	private static AnonInfluxDBImpl getErrorThrowingDB() {
		return new AnonInfluxDBImpl(true);
	}
	
	private static QueueDepthRecordingDBImpl getQueueDepthRecordingDBImpl() {
		return new QueueDepthRecordingDBImpl();
	}
	
	private final String ANON_DB = "db";
	private final String ANON_RETENTION = "default";
	private final ConsistencyLevel ANON_CONSISTENCY = ConsistencyLevel.ONE;

	@Test(expectedExceptions={IllegalArgumentException.class})
	public void cannotBuildWithActionsGreaterThanCapacity() {
		BatchProcessor.builder(getAnonInfluxDB())
		.capacityAndActions(1, 2)
		.build();
	}

	@Test(expectedExceptions={IllegalStateException.class})
    public void addingThrowsExceptionWhenBehaviourIsThrowExceptionAndQueueAtCapacity() {
    	BatchProcessor subject = BatchProcessor.builder(getErrorThrowingDB())
    			.interval(1, 2, TimeUnit.DAYS)
    			.capacityAndActions(1, 1)
    			.discardOnFailedWrite(false)
    			.behaviour(BufferFailBehaviour.THROW_EXCEPTION)
    			.build();
    	
    	boolean putResult;
    	putResult = subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
    	Assert.assertTrue(putResult);
    	subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
    }
    
	@Test
	public void addingEvictsOldestWhenBehaviourIsDropOldestAndQueueAtCapacity() {
		BatchProcessor subject = BatchProcessor.builder(getErrorThrowingDB())
				.interval(1, 2, TimeUnit.DAYS)
				.capacityAndActions(1, 1)
				.discardOnFailedWrite(false)
    			.behaviour(BufferFailBehaviour.DROP_OLDEST)
				.build();
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure1"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
    	
    	subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure2"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure2");
    	
    	subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure3"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure3");
    	
    }
    
	@Test
    public void addingDoesNotInsertCurrentWhenBehaviourIsDropCurrentAndKeepOnFailedWriteAndQueueAtCapacity() {
    	BatchProcessor subject = new NonScheduledWriteBatchProcessor(getErrorThrowingDB(), 1, TimeUnit.SECONDS, 1, 1, BufferFailBehaviour.DROP_CURRENT, false, 50);
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure1"));
//		subject.attemptWrite();
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
    	
    	subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure2"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
    	
    	subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure3"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
        
    }
    
	@Test(expectedExceptions = { IllegalArgumentException.class })
    public void cannotBeBuiltWithDropOldestBehaviourAndWithoutCapacityLimit() {
		BatchProcessor.builder(getAnonInfluxDB())
			.interval(1, 2, TimeUnit.DAYS)
			.behaviour(BufferFailBehaviour.DROP_OLDEST)
			.build();
		
    }
	
	@Test
    public void pointsAreRemovedFromQueueAfterSuccessfulWrite() {
		BatchProcessor subject = BatchProcessor.builder(getAnonInfluxDB())
			.interval(1, 2, TimeUnit.DAYS)
			.build();
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 1);
		subject.write();
		Assert.assertEquals(subject.queue.size(), 0);	
    }
	
	@Test
    public void keepOnFailedWriteProcessorRetainsPointsAfterExceptionThrown() {
		BatchProcessor subject = BatchProcessor.builder(getErrorThrowingDB())
			.interval(1, 2, TimeUnit.DAYS)
			.discardOnFailedWrite(false)
			.build();
		
		Point point = getAnonPoint();
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, point);
		Assert.assertEquals(subject.queue.size(), 1);
		subject.write();
		Assert.assertEquals(subject.queue.size(), 1);
		// TODO this is bad, Law of Demeter violation!
		Assert.assertEquals(subject.queue.peek().getPoint(), point);
    }
	
	@Test
    public void discardOnFailedWriteProcessorDropsPointsAfterExceptionThrown() {
		BatchProcessor subject = BatchProcessor.builder(getErrorThrowingDB())
			.interval(1, 2, TimeUnit.DAYS)
			.discardOnFailedWrite(true)
			.build();
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 1);
		subject.write();
		Assert.assertEquals(subject.queue.size(), 0);	
    }
	
	@Test
    public void writeCalledAfterActionsReached() {
		AnonInfluxDBImpl influxDb = getAnonInfluxDB();
//		BatchProcessor subject = BatchProcessor.builder(influxDb)
//			.interval(1, TimeUnit.DAYS)
//			.actions(2)
//			.build();
		BatchProcessor subject = new NonScheduledWriteBatchProcessor(influxDb, 2, TimeUnit.DAYS, 1, null, 
				BufferFailBehaviour.THROW_EXCEPTION, false, 50);

		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 1);
		Assert.assertEquals(influxDb.writeCalled, 0);
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 0);
		Assert.assertEquals(influxDb.writeCalled, 1);	
    }
	
	@Test
    public void writeNotCascadedAfterWriteFailure() {
		AnonInfluxDBImpl influxDb = getErrorThrowingDB();
//		BatchProcessor subject = BatchProcessor.builder(influxDb)
//			.interval(1, TimeUnit.DAYS)
//			.capacityAndActions(3, 1)
//			.discardOnFailedWrite(false)
//			.build();
		BatchProcessor subject = new NonScheduledWriteBatchProcessor(influxDb, 1, TimeUnit.DAYS, 1, 3, 
				BufferFailBehaviour.THROW_EXCEPTION, false, 50);

		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 1);
		Assert.assertEquals(influxDb.writeCalled, 1);
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 2);
		Assert.assertEquals(influxDb.writeCalled, 1);
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 3);
		Assert.assertEquals(influxDb.writeCalled, 1);
    }

	@Test
	public void successfullyWrittenPointsAreNotReturnedToQueue() {
		AnonInfluxDBImpl influxDb = getAnonInfluxDB();
		BatchProcessor subject = new NonScheduledWriteBatchProcessor(influxDb, 3, TimeUnit.DAYS, 1, 3, BufferFailBehaviour.DROP_CURRENT, false, 50);
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure1"));
		subject.put(AnonInfluxDBImpl.FAIL_DATABASE, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure3"));
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure2"));
		Assert.assertEquals(influxDb.writeCalled, 2); // Once for ANON_TB, once for FAIL_DATABASE
		Assert.assertEquals(subject.queue.size(), 1);
		Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure3");
	}
	
	@Test
	public void unsuccessfullyWrittenPointsAreReturnedToQueueInCorrectOrder() {
		AnonInfluxDBImpl influxDb = getErrorThrowingDB();
		BatchProcessor subject = new NonScheduledWriteBatchProcessor(influxDb, 4, TimeUnit.DAYS, 1, 4, BufferFailBehaviour.THROW_EXCEPTION, false, 50);
		
		subject.put("db1", ANON_RETENTION, ANON_CONSISTENCY, getPoint("inserted1"));
		subject.put("db2", ANON_RETENTION, ANON_CONSISTENCY, getPoint("inserted2"));
		subject.put("db2", ANON_RETENTION, ANON_CONSISTENCY, getPoint("inserted3"));
		subject.put("db1", ANON_RETENTION, ANON_CONSISTENCY, getPoint("inserted4"));
		
		Assert.assertEquals(influxDb.writeCalled, 2); // Once for db1, once for db2
		Assert.assertEquals(subject.queue.size(), 4);
		Assert.assertEquals(subject.queue.peekFirst().getPoint().getMeasurement(), "inserted1");
		Assert.assertEquals(subject.queue.peekLast().getPoint().getMeasurement(), "inserted4");
	}
	
	@Test
	public void writeOnlyAttemptsUpToMaxBatchWrite() {
		AnonInfluxDBImpl influxDb = getAnonInfluxDB();
//		BatchProcessor subject = BatchProcessor.builder(influxDb)
//			.interval(1, TimeUnit.DAYS)
//			.capacityAndActions(3, 3)
//			.maxBatchWriteSize(2)
//			.discardOnFailedWrite(false)
//			.build();
		BatchProcessor subject = new NonScheduledWriteBatchProcessor(influxDb, 3, TimeUnit.DAYS, 1, 3, 
				BufferFailBehaviour.THROW_EXCEPTION, false, 2);

		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure1"));
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure2"));
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure3"));
		
		Assert.assertEquals(influxDb.writeCalled, 1);
		Assert.assertEquals(subject.queue.size(), 1);
		Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure3");
	}
	
	@Test 
	public void testGetBufferedCountWorksInTheMiddleOfAWrite() {
		QueueDepthRecordingDBImpl influxDb = getQueueDepthRecordingDBImpl();
		int capacity = 5;
		int flushActions = 5;
		int flushIntervalMin = 5;
		int flushIntervalMax = 2* flushIntervalMin;
		boolean discardOnFailedWrite = false;
		int maxBatchWriteSize = 5;
		influxDb.enableBatch(capacity, flushActions, flushIntervalMin, flushIntervalMax,
				TimeUnit.SECONDS, BufferFailBehaviour.THROW_EXCEPTION, discardOnFailedWrite, maxBatchWriteSize);
		BatchProcessor subject = influxDb.getBatchProcessor();
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure1"));
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure2"));
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure3"));		
		Assert.assertEquals(subject.queue.size(), 3);
		Assert.assertEquals(subject.getBufferedCount(), 3);

		subject.write();
		Assert.assertEquals(influxDb.writeCalled, 1);
		Assert.assertEquals(influxDb.getQueueDepth(), 3);
		Assert.assertEquals(subject.queue.size(), 0);
		Assert.assertEquals(subject.getBufferedCount(), 0);
	}
}

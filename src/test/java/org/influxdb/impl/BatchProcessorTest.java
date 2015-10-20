package org.influxdb.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.Point;
import org.influxdb.impl.BatchProcessor.BufferFailBehaviour;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BatchProcessorTest {
	private static class AnonInfluxDBImpl extends InfluxDBImpl {
		private final boolean throwErrorOnWriteBatched;

		public AnonInfluxDBImpl(final boolean throwErrorOnWriteBatched) {
			super("temp", "user", "pass");
			this.throwErrorOnWriteBatched = throwErrorOnWriteBatched;
		}
		
		@Override
		protected void writeBatched(String database, String retentionPolicy, ConsistencyLevel consistencyLevel,
				List<Point> points) {
			if (throwErrorOnWriteBatched) {
				throw new RuntimeException("Anon error");
			}
		}
		
		@Override
		protected void writeUnbatched(String database, String retentionPolicy, ConsistencyLevel consistencyLevel,
				Point point) {
		}
	}

	private static Point getAnonPoint() {
		return getPoint("anon");
	}
	
	private static Point getPoint(String measurement) {
		return Point.measurement(measurement)
    			.field("field", "value").build();
	}
	
	private static InfluxDBImpl ANON_INFLUXDB = new AnonInfluxDBImpl(false); 
	private static InfluxDBImpl ERROR_THROWING_INFLUXDB = new AnonInfluxDBImpl(true); 
	private final String ANON_DB = "db";
	private final String ANON_RETENTION = "default";
	private final ConsistencyLevel ANON_CONSISTENCY = ConsistencyLevel.ONE;


	@Test(expectedExceptions={IllegalStateException.class})
    public void addingThrowsExceptionWhenDepthExceededAndBehaviourIsThrowException() {
    	BatchProcessor subject = BatchProcessor.builder(ANON_INFLUXDB)
    			.interval(1, TimeUnit.DAYS)
    			.capacity(1)
    			.behaviour(BufferFailBehaviour.THROW_EXCEPTION)
    			.build();
    	boolean putResult = subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY,
    			getAnonPoint());
    	Assert.assertTrue(putResult);
    	subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
    }
    
	@Test
	public void addingEvictsOldestWhenDepthExceededAndBehaviourIsDropOldest() {
		BatchProcessor subject = BatchProcessor.builder(ANON_INFLUXDB)
				.interval(1, TimeUnit.DAYS)
				.capacity(1)
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
    public void addingDoesNotInsertCurrentWhenDepthExceededAndBehaviourIsDropCurrent() {
    	BatchProcessor subject = BatchProcessor.builder(ANON_INFLUXDB)
			.interval(1, TimeUnit.DAYS)
			.capacity(1)
			.behaviour(BufferFailBehaviour.DROP_CURRENT)
			.build();
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure1"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
    	
    	subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure2"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
    	
    	subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getPoint("measure3"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
        
    }
    
	@Test(expectedExceptions = { IllegalArgumentException.class })
    public void cannotBeBuiltWithInfiniteDepthAndDropOldestBehaviour() {
		BatchProcessor.builder(ANON_INFLUXDB)
			.interval(1, TimeUnit.DAYS)
			.behaviour(BufferFailBehaviour.DROP_OLDEST)
			.build();
		
    }
	
	@Test
    public void pointsRemovedFromQueueAfterSuccessfulWrite() {
		BatchProcessor subject = BatchProcessor.builder(ANON_INFLUXDB)
			.interval(1, TimeUnit.DAYS)
			.build();
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 1);
		subject.write();
		Assert.assertEquals(subject.queue.size(), 0);	
    }
	
	@Test
    public void keepOnFailedWriteProcessorRetainsPointsAfterExceptionThrown() {
		BatchProcessor subject = BatchProcessor.builder(ERROR_THROWING_INFLUXDB)
			.interval(1, TimeUnit.DAYS)
			.discardOnFailedWrite(false)
			.build();
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 1);
		subject.write();
		Assert.assertEquals(subject.queue.size(), 1);	
    }
	
	@Test
    public void discardOnFailedWriteProcessorDropsPointsAfterExceptionThrown() {
		BatchProcessor subject = BatchProcessor.builder(ERROR_THROWING_INFLUXDB)
			.interval(1, TimeUnit.DAYS)
			.discardOnFailedWrite(true)
			.build();
		
		subject.put(ANON_DB, ANON_RETENTION, ANON_CONSISTENCY, getAnonPoint());
		Assert.assertEquals(subject.queue.size(), 1);
		subject.write();
		Assert.assertEquals(subject.queue.size(), 0);	
    }

	public static void main(String[] args) {
		(new BatchProcessorTest()).pointsRemovedFromQueueAfterSuccessfulWrite();
		(new BatchProcessorTest()).discardOnFailedWriteProcessorDropsPointsAfterExceptionThrown();
	}
}

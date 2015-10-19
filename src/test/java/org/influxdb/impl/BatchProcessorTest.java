package org.influxdb.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.Point;
import org.influxdb.impl.BatchProcessor.BufferFailBehaviour;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BatchProcessorTest {
	private class AnonInfluxDBImpl extends InfluxDBImpl {

		public AnonInfluxDBImpl() {
			super("temp", "user", "pass");
		}
		
		@Override
		protected void writeBatched(String database, String retentionPolicy, ConsistencyLevel consistencyLevel,
				List<Point> points) {
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

	@Test(expectedExceptions={IllegalStateException.class})
    public void addingThrowsExceptionWhenDepthExceededAndBehaviourIsThrowException() {
    	BatchProcessor subject = BatchProcessor.builder(new AnonInfluxDBImpl())
    			.interval(1, TimeUnit.DAYS)
    			.capacity(1)
    			.behaviour(BufferFailBehaviour.THROW_EXCEPTION)
    			.build();
    	boolean putResult = subject.put("db", "default", ConsistencyLevel.ALL,
    			getAnonPoint());
    	Assert.assertTrue(putResult);
    	subject.put("db", "default", ConsistencyLevel.ALL, getAnonPoint());
    }
    
	@Test
	public void addingEvictsOldestWhenDepthExceededAndBehaviourIsDropOldest() {
		BatchProcessor subject = BatchProcessor.builder(new AnonInfluxDBImpl())
				.interval(1, TimeUnit.DAYS)
				.capacity(1)
				.behaviour(BufferFailBehaviour.DROP_OLDEST)
				.build();
		
		subject.put("db", "default", ConsistencyLevel.ALL, getPoint("measure1"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
    	
    	subject.put("db", "default", ConsistencyLevel.ALL, getPoint("measure2"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure2");
    	
    	subject.put("db", "default", ConsistencyLevel.ALL, getPoint("measure3"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure3");
    	
    }
    
	@Test
    public void addingDoesNotInsertCurrentWhenDepthExceededAndBehaviourIsDropCurrent() {
    	BatchProcessor subject = BatchProcessor.builder(new AnonInfluxDBImpl())
			.interval(1, TimeUnit.DAYS)
			.capacity(1)
			.behaviour(BufferFailBehaviour.DROP_CURRENT)
			.build();
		
		subject.put("db", "default", ConsistencyLevel.ALL, getPoint("measure1"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
    	
    	subject.put("db", "default", ConsistencyLevel.ALL, getPoint("measure2"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
    	
    	subject.put("db", "default", ConsistencyLevel.ALL, getPoint("measure3"));
    	Assert.assertEquals(subject.queue.peek().getPoint().getMeasurement(), "measure1");
        
    }
    
	@Test(expectedExceptions = { IllegalArgumentException.class })
    public void cannotBeBuiltWithInfiniteDepthAndDropOldestBehaviour() {
		BatchProcessor.builder(new AnonInfluxDBImpl())
			.interval(1, TimeUnit.DAYS)
			.behaviour(BufferFailBehaviour.DROP_OLDEST)
			.build();
		
    }

}

package org.influxdb.dto;

import static org.testng.AssertJUnit.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.math.RandomUtils;
import org.influxdb.dto.Point.Builder;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ObjectSizeTest {

	// PRIVATE
	private static long fSLEEP_INTERVAL = 100;

	private long getMemoryUse() {
		putOutTheGarbage();
		long totalMemory = Runtime.getRuntime().totalMemory();
		putOutTheGarbage();
		long freeMemory = Runtime.getRuntime().freeMemory();
		return (totalMemory - freeMemory);
	}

	private void putOutTheGarbage() {
		collectGarbage();
		collectGarbage();
	}

	private void collectGarbage() {
		try {
			System.gc();
			Thread.currentThread();
			Thread.sleep(fSLEEP_INTERVAL);
			System.runFinalization();
			Thread.currentThread();
			Thread.sleep(fSLEEP_INTERVAL);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
	}
	
	private Map<String, String> getTags(int i) {
		Map<String, String> map = Maps.newHashMap();
		
		map.put("tag1", "tagA");
		
		map.put("alternatingTag", (i % 2 == 0) ? "tagValue123" : "tagvalueBCD");
		
//		String commonButLong = "baseTag";
//		for (int iter=0; iter < (i%5); iter++) {
//			commonButLong += "baseTag";
//		}
//		map.put("commonButLong", commonButLong);
		
//		if (i % 15 ==  0) {
//		    map.put("infrequentTag1", "almostNever");
//		}
//		
//		if (i % 42 == 0) {
//			map.put("infrequentTag2", "theMeaningOfLife");
//		}
		
		return map;
	}
	
	private Map<String, Object> getFields(int i) {
		Map<String, Object> map = Maps.newHashMap();
		
		// Add fields
		map.put("value", RandomUtils.nextLong());
//		double doubleVal = RandomUtils.nextDouble();
//		if (i % 2 == 0) {
//			if (i % 4 == 0) {
//				map.put("value2", doubleVal);
//			} else {
//				map.put("value2", String.valueOf(doubleVal));
//			}
//		}
		
		return map;
	}
	
	private Point getPoint(Map<String, String> tags, Map<String, Object> fields) {
		Builder b = Point.measurement("measurement");

		b.tag(tags);
		b.fields(fields);
		
		return b.build();
	}

	private Point2 getPoint2(Map<String, String> tags, Map<String, Object> fields) {
		org.influxdb.dto.Point2.Builder b = Point2.measurement("measurement");

		b.tag(tags);
		b.fields(fields);
		
		return b.build();
	}

	@Test
	public void testSizeOfPointObjects() {
		int NUM_POINTS = 200000;
		List<Point> points = Lists.newArrayList();
		List<Point2> soPoints = Lists.newArrayList();

		long before, middle, after;
		before = getMemoryUse();

		for (int i = 0; i < NUM_POINTS; i++) {
			points.add(getPoint(getTags(i), getFields(i)));
		}

		middle = getMemoryUse();
		
		for (int i = 0; i < NUM_POINTS; i++) {
			soPoints.add(getPoint2(getTags(i), getFields(i)));
		}
		
		after = getMemoryUse();
		assertNotNull(points);
		assertNotNull(soPoints);
		
		Long pointBytes = new Long(middle - before);
		Long soPointBytes = new Long(after - middle);
		System.out.println("Point>" + pointBytes);
		System.out.println("SOPoint>" + soPointBytes);
		
		Long delta = Math.abs(pointBytes - soPointBytes);
		System.out.println("Bytes per object Point>" + pointBytes/NUM_POINTS);
		System.out.println("Bytes per object SOPoint>" + soPointBytes/NUM_POINTS);
		System.out.println("Bytes per object improvement>" + delta/NUM_POINTS);
	}

}


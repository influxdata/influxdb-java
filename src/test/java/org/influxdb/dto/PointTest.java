package org.influxdb.dto;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.Assert;
import org.testng.annotations.Test;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Test for the Point DTO.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class PointTest {

	/**
	 * Test that lineprotocol is conformant to:
	 * 
	 * https://github.com/influxdb/influxdb/blob/master/tsdb/README.md
	 *
	 */
	@Test
	public void lineProtocol() {
		Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", 1).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=1.0 1");

		point = Point.measurement("test,1").time(1, TimeUnit.NANOSECONDS).field("a", 1).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test\\,1 a=1.0 1");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", "A").build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=\"A\" 1");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", "A\"B").build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=\"A\\\"B\" 1");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", "A\"B\"C").build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=\"A\\\"B\\\"C\" 1");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", "A B C").build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=\"A B C\" 1");

		point = Point
				.measurement("test")
				.time(1, TimeUnit.NANOSECONDS)
				.field("a", "A\"B")
				.field("b", "D E \"F")
				.build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=\"A\\\"B\",b=\"D E \\\"F\" 1");

	}

	/**
	 * Test for ticket #44
	 */
	@Test(enabled = true)
	public void testTicket44() {
		Point point = Point.measurement("test").time(1, TimeUnit.MICROSECONDS).field("a", 1).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=1.0 1000");

		point = Point.measurement("test").time(1, TimeUnit.MILLISECONDS).field("a", 1).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=1.0 1000000");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", 1).build();
		BatchPoints batchPoints = BatchPoints.database("db").point(point).build();
		Assert.assertEquals(batchPoints.lineProtocol(), "test a=1.0 1\n");

		point = Point.measurement("test").time(1, TimeUnit.MICROSECONDS).field("a", 1).build();
		batchPoints = BatchPoints.database("db").point(point).build();
		Assert.assertEquals(batchPoints.lineProtocol(), "test a=1.0 1000\n");

		point = Point.measurement("test").time(1, TimeUnit.MILLISECONDS).field("a", 1).build();
		batchPoints = BatchPoints.database("db").point(point).build();
		Assert.assertEquals(batchPoints.lineProtocol(), "test a=1.0 1000000\n");

		point = Point.measurement("test").field("a", 1).time(1, TimeUnit.MILLISECONDS).build();
		batchPoints = BatchPoints.database("db").build();
		batchPoints = batchPoints.point(point);
		Assert.assertEquals(batchPoints.lineProtocol(), "test a=1.0 1000000\n");

	}

	/**
	 * Test for ticket #54
	 */
	@Test
	public void testTicket54() {
		Byte byteNumber = 100;
		Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", byteNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=100.0 1");

		int intNumber = 100000000;
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", intNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=100000000.0 1");

		Integer integerNumber = 100000000;
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", integerNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=100000000.0 1");

		AtomicInteger atomicIntegerNumber = new AtomicInteger(100000000);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", atomicIntegerNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=100000000.0 1");

		Long longNumber = 1000000000000000000L;
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", longNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=1000000000000000000.0 1");

		AtomicLong atomicLongNumber = new AtomicLong(1000000000000000000L);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", atomicLongNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=1000000000000000000.0 1");

		BigInteger bigIntegerNumber = BigInteger.valueOf(100000000);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", bigIntegerNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=100000000.0 1");

		Double doubleNumber = Double.valueOf(100000000.0001);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", doubleNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=100000000.0001 1");

		Float floatNumber = Float.valueOf(0.1f);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", floatNumber).build();
		Assert.assertTrue(point.lineProtocol().toString().startsWith("test a=0.10"));

		BigDecimal bigDecimalNumber = BigDecimal.valueOf(100000000.00000001);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", bigDecimalNumber).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test a=100000000.00000001 1");
	}
	
	@Test
	public void testEscapingOfKeysAndValues() {
		// Test escaping of spaces
		Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("foo", "bar baz").field( "a", 1 ).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test,foo=bar\\ baz a=1.0 1");
 
		// Test escaping of commas
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("foo", "bar,baz").field( "a", 1 ).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test,foo=bar\\,baz a=1.0 1");

		// Test escaping of equals sign
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("foo", "bar=baz").field( "a", 1 ).build();
		Assert.assertEquals(point.lineProtocol().toString(), "test,foo=bar\\=baz a=1.0 1");
	}

}

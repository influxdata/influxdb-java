package org.influxdb.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

import com.google.common.collect.Maps;

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
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=1.0 1");

		point = Point.measurement("test,1").time(1, TimeUnit.NANOSECONDS).field("a", 1).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test\\,1 a=1.0 1");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", "A").build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=\"A\" 1");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", "A\"B").build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=\"A\\\"B\" 1");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", "A\"B\"C").build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=\"A\\\"B\\\"C\" 1");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", "A B C").build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=\"A B C\" 1");

		point = Point
				.measurement("test")
				.time(1, TimeUnit.NANOSECONDS)
				.field("a", "A\"B")
				.field("b", "D E \"F")
				.build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=\"A\\\"B\",b=\"D E \\\"F\" 1");

		//Integer type
		point = Point.measurement("inttest").useInteger(true).time(1, TimeUnit.NANOSECONDS).field("a", (Integer)1).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("inttest a=1i 1");

		point = Point.measurement("inttest,1").useInteger(true).time(1, TimeUnit.NANOSECONDS).field("a", (Integer)1).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("inttest\\,1 a=1i 1");

		point = Point.measurement("inttest,1").useInteger(true).time(1, TimeUnit.NANOSECONDS).field("a", 1L).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("inttest\\,1 a=1i 1");

		point = Point.measurement("inttest,1").useInteger(true).time(1, TimeUnit.NANOSECONDS).field("a", BigInteger.valueOf(100)).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("inttest\\,1 a=100i 1");
	}

	/**
	 * Test for ticket #44
	 */
	@Test(enabled = true)
	public void testTicket44() {
		Point point = Point.measurement("test").time(1, TimeUnit.MICROSECONDS).field("a", 1).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=1.0 1000");

		point = Point.measurement("test").time(1, TimeUnit.MILLISECONDS).field("a", 1).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=1.0 1000000");

		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", 1).build();
		BatchPoints batchPoints = BatchPoints.database("db").point(point).build();
		assertThat(batchPoints.lineProtocol()).asString().isEqualTo("test a=1.0 1\n");

		point = Point.measurement("test").time(1, TimeUnit.MICROSECONDS).field("a", 1).build();
		batchPoints = BatchPoints.database("db").point(point).build();
		assertThat(batchPoints.lineProtocol()).asString().isEqualTo("test a=1.0 1000\n");

		point = Point.measurement("test").time(1, TimeUnit.MILLISECONDS).field("a", 1).build();
		batchPoints = BatchPoints.database("db").point(point).build();
		assertThat(batchPoints.lineProtocol()).asString().isEqualTo("test a=1.0 1000000\n");

		point = Point.measurement("test").field("a", 1).time(1, TimeUnit.MILLISECONDS).build();
		batchPoints = BatchPoints.database("db").build();
		batchPoints = batchPoints.point(point);
		assertThat(batchPoints.lineProtocol()).asString().isEqualTo("test a=1.0 1000000\n");

	}

	/**
	 * Test for ticket #54
	 */
	@Test
	public void testTicket54() {
		Byte byteNumber = 100;
		Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", byteNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=100.0 1");

		int intNumber = 100000000;
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", intNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=100000000.0 1");

		Integer integerNumber = 100000000;
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", integerNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=100000000.0 1");

		AtomicInteger atomicIntegerNumber = new AtomicInteger(100000000);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", atomicIntegerNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=100000000.0 1");

		Long longNumber = 1000000000000000000L;
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", longNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=1000000000000000000.0 1");

		AtomicLong atomicLongNumber = new AtomicLong(1000000000000000000L);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", atomicLongNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=1000000000000000000.0 1");

		BigInteger bigIntegerNumber = BigInteger.valueOf(100000000);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", bigIntegerNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=100000000.0 1");

		Double doubleNumber = Double.valueOf(100000000.0001);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", doubleNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=100000000.0001 1");

		Float floatNumber = Float.valueOf(0.1f);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", floatNumber).build();
		assertThat(point.lineProtocol()).asString().startsWith("test a=0.10");

		BigDecimal bigDecimalNumber = BigDecimal.valueOf(100000000.00000001);
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).field("a", bigDecimalNumber).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=100000000.00000001 1");
	}
	
	@Test
	public void testEscapingOfKeysAndValues() {
		// Test escaping of spaces
		Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("foo", "bar baz").field( "a", 1 ).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test,foo=bar\\ baz a=1.0 1");
 
		// Test escaping of commas
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("foo", "bar,baz").field( "a", 1 ).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test,foo=bar\\,baz a=1.0 1");

		// Test escaping of equals sign
		point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("foo", "bar=baz").field( "a", 1 ).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test,foo=bar\\=baz a=1.0 1");
	}

	/**
	 * Test for issue #117.
	 */
	@Test
	public void testIgnoreNullPointerValue() {
		// Test omission of null values
		Point.Builder pointBuilder = Point.measurement("nulltest").time(1, TimeUnit.NANOSECONDS).tag("foo", "bar");

		pointBuilder.field("field1", "value1");
		pointBuilder.field("field2", null);
		pointBuilder.field("field3", (Integer) 1);

		Point point = pointBuilder.build();

		assertThat(point.lineProtocol()).asString().isEqualTo("nulltest,foo=bar field1=\"value1\",field3=1.0 1");
	}
	
	/**
	 * Tests for issue #110
	 */
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testAddingTagsWithNullNameThrowsAnError() {
		Point.measurement("dontcare").tag(null, "DontCare");
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testAddingTagsWithNullValueThrowsAnError() {
		Point.measurement("dontcare").tag("DontCare", null);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testAddingMapOfTagsWithNullNameThrowsAnError() {
		Map<String, String> map = Maps.newHashMap();
		map.put(null, "DontCare");
		Point.measurement("dontcare").tag(map);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testAddingMapOfTagsWithNullValueThrowsAnError() {
		Map<String, String> map = Maps.newHashMap();
		map.put("DontCare", null);
		Point.measurement("dontcare").tag(map);
	}
	
	
}

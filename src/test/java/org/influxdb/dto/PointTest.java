package org.influxdb.dto;

import org.testng.annotations.Test;
import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.TimeUnit;

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
		assertThat(point.lineProtocol()).asString().isEqualTo("test a=1 1");

		point = Point.measurement("test,1").time(1, TimeUnit.NANOSECONDS).field("a", 1).build();
		assertThat(point.lineProtocol()).asString().isEqualTo("test\\,1 a=1 1");

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

	}

}

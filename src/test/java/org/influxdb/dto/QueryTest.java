package org.influxdb.dto;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for the Query DTO.
 *
 * @author jord [at] moz.com
 *
 */
public class QueryTest {

	/**
	 * Test that equals does what it is supposed to do.
	 */
	@Test
	public void testEqualsAndHashCode() {
		String stringA0 = "thesame";
		String stringA1 = "thesame";
		String stringB0 = "notthesame";

		Query queryA0 = new Query(stringA0, stringA0);
		Query queryA1 = new Query(stringA1, stringA1);
		Query queryB0 = new Query(stringA0, stringB0);
		Query queryC0 = new Query(stringB0, stringA0);

		Assert.assertEquals(queryA0, queryA1);
		Assert.assertNotEquals(queryA0, queryB0);
		Assert.assertNotEquals(queryB0, queryC0);

		Assert.assertEquals(queryA0.hashCode(), queryA1.hashCode());
		Assert.assertNotEquals(queryA0.hashCode(), queryB0.hashCode());
	}
}

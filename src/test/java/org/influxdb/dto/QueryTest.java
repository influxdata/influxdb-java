package org.influxdb.dto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;


/**
 * Test for the Query DTO.
 *
 * @author jord [at] moz.com
 *
 */
@RunWith(JUnitPlatform.class)
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

		assertThat(queryA0).isEqualTo(queryA1);
		assertThat(queryA0).isNotEqualTo(queryB0);
		assertThat(queryB0).isNotEqualTo(queryC0);

		assertThat(queryA0.hashCode()).isEqualTo(queryA1.hashCode());
		assertThat(queryA0.hashCode()).isNotEqualTo(queryB0.hashCode());
	}

	/**
	 * Test encode does what it is supposed to do.
	 */
	@Test
	public void testEncode() throws UnsupportedEncodingException {
		String queryString1 = "SELECT * FROM cpu";
		String queryString2 = "SELECT * FROM cpu;SELECT * FROM cpu";

		String encodedQueryString1 = Query.encode(queryString1);
		String encodedQueryString2 = Query.encode(queryString2);

		assertThat(decode(encodedQueryString1)).isEqualTo(queryString1);
		assertThat(decode(encodedQueryString2)).isEqualTo(queryString2);
		assertThat(encodedQueryString2).doesNotContain(";");
	}

	/**
	 * Test getCommandWithUrlEncoded does what it is supposed to do.
	 */
	@Test
	public void testGetCommandWithUrlEncoded() throws UnsupportedEncodingException {
		String queryString1 = "CREATE DATABASE \"testdb\"";
		String queryString2 = "SELECT * FROM cpu;SELECT * FROM cpu;";
		String queryString3 = "%3B%2B%";
		String queryString4 = "non_escape";
		String database = "testdb";

		Query query1 = new Query(queryString1, database);
		Query query2 = new Query(queryString2, database);
		Query query3 = new Query(queryString3, database);
		Query query4 = new Query(queryString4, database);

		assertThat(query1.getCommandWithUrlEncoded()).isNotEqualTo(query1.getCommand());
		assertThat(decode(query1.getCommandWithUrlEncoded())).isEqualTo(query1.getCommand());

		assertThat(query2.getCommandWithUrlEncoded()).isNotEqualTo(query2.getCommand());
		assertThat(decode(query2.getCommandWithUrlEncoded())).isEqualTo(query2.getCommand());

		assertThat(query3.getCommandWithUrlEncoded()).isNotEqualTo(query3.getCommand());
		assertThat(decode(query3.getCommandWithUrlEncoded())).isEqualTo(query3.getCommand());

		assertThat(query4.getCommandWithUrlEncoded()).isEqualTo(query4.getCommand());
		assertThat(decode(query4.getCommandWithUrlEncoded())).isEqualTo(query4.getCommand());
	}

	private static String decode(String str) throws UnsupportedEncodingException {
		return URLDecoder.decode(str, StandardCharsets.UTF_8.toString());
	}
}

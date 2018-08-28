package org.influxdb.querybuilder;

import org.influxdb.dto.Query;
import org.junit.jupiter.api.Test;

import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BuiltQueryTest {

	private static final String DATABASE = "testdb";

	@Test
	public void testAlias() {
		Query query = new Query("SELECT MAX(k) AS hello FROM foo;", DATABASE);
		Query select = select().max("k").as("hello").from(DATABASE,"foo");

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testRegex() {
		Query query = new Query("SELECT MAX(k) FROM foo WHERE k =~ /[0-9]/;",DATABASE);
		Query select = select().max("k").from(DATABASE,"foo").where(regex("k", "/[0-9]/"));

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testNegativeRegex() {
		Query query = new Query("SELECT MAX(k) FROM foo WHERE k ~! /[0-9]/;",DATABASE);
		Query select = select().max("k").from(DATABASE,"foo").where(nregex("k", "/[0-9]/"));

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testContains() {
		Query query = new Query("SELECT MAX(k) FROM foo WHERE k =~ /*text*/;",DATABASE);
		Query select = select().max("k").from(DATABASE,"foo").where(contains("k", "text"));

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testDistinct() {
		Query query = new Query("SELECT DISTINCT k FROM foo;",DATABASE);
		Query select = select().column("k").distinct().from(DATABASE ,"foo");

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testDistinctWithExpression() {
		Query query = new Query("SELECT DISTINCT COUNT(test1) FROM foo LIMIT 1 OFFSET 20;", DATABASE);
		Query select = select().count("test1").distinct().from(DATABASE ,"foo").limit(1, 20);

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testMultipleColumns() {
		Query query = select().column("test1").distinct().column("test2").from("foo");
		assertThrows(IllegalStateException.class, () -> query.getCommand(), "Cannot mix all columns and specific columns");
	}

	@Test
	public void testOrdering() {

		Query query = new Query("SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z' ORDER BY time ASC;",DATABASE);

		Select select = select().all().from(DATABASE,"foo")
				.where(eq("k", 4))
				.and(gt("c", "a"))
				.and(lte("c", "z"))
				.orderBy(asc());

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testSelect() {
		Query query = new Query("SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';",DATABASE);
		Query select = select().all().from(DATABASE,"foo").where(eq("k", 4)).and(gt("c", "a")).and(lte("c", "z"));

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testMean() {
		Query query = new Query("SELECT MEAN(k) FROM foo WHERE k=4 AND c>'a' AND c<='z';",DATABASE);
		Query select = select().mean("k")
				.from(DATABASE,"foo")
				.where(eq("k", 4))
				.and(gt("c", "a"))
				.and(lte("c", "z"));

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testSum() {
		Query query = new Query("SELECT SUM(k) FROM foo WHERE k=4 AND c>'a' AND c<='z';",DATABASE);
		Query select = select().sum("k")
				.from(DATABASE,"foo")
				.where(eq("k", 4))
				.and(gt("c", "a"))
				.and(lte("c", "z"));

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testMin() {
		Query query = new Query("SELECT MIN(k) FROM foo WHERE k=4 AND c>'a' AND c<='z';",DATABASE);
		Query select = select().min("k")
				.from(DATABASE,"foo")
				.where(eq("k", 4))
				.and(gt("c", "a"))
				.and(lte("c", "z"));

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

	@Test
	public void testMax() {
		Query query = new Query("SELECT MAX(k) FROM foo WHERE k=4 AND c>'a' AND c<='z';",DATABASE);
		Query select = select().max("k")
				.from(DATABASE,"foo")
				.where(eq("k", 4))
				.and(gt("c", "a"))
				.and(lte("c", "z"));

		assertEquals(query.getCommand(),select.getCommand());
		assertEquals(query.getDatabase(),select.getDatabase());
	}

}



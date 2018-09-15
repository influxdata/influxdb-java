package org.influxdb.querybuilder;

import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.*;
import static org.influxdb.querybuilder.Operations.ADD;
import static org.influxdb.querybuilder.Operations.SUB;
import static org.influxdb.querybuilder.time.DurationLiteral.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.influxdb.dto.Query;
import org.junit.jupiter.api.Test;

public class BuiltQueryTest {

  private static final String DATABASE = "testdb";

  @Test
  public void testCommandWithUrlEncoded() {
    Query select = select().max("test1").as("hello").from(DATABASE, "foobar");
    String encoded = select.getCommandWithUrlEncoded();

    assertEquals("SELECT+MAX%28test1%29+AS+hello+FROM+foobar%3B", encoded);
  }

  @Test
  public void testQueryWithoutTable() {
    Query select = select().max("test1").as("hello").from(DATABASE, null);
    assertThrows(IllegalArgumentException.class, () -> select.getCommand());
  }

  @Test
  public void testAlias() {
    Query query = new Query("SELECT MAX(test1) AS hello FROM foobar;", DATABASE);
    Query select = select().max("test1").as("hello").from(DATABASE, "foobar");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testRegex() {
    Query query = new Query("SELECT MAX(test1) FROM foobar WHERE test1 =~ /[0-9]/;", DATABASE);
    Query select = select().max("test1").from(DATABASE, "foobar").where(regex("test1", "/[0-9]/"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testInvalidRegex() {
    assertThrows(
        IllegalArgumentException.class,
        () -> select().max("test1").from(DATABASE, "foobar").where(regex("test1", null)),
        "Missing text for expression");
  }

  @Test
  public void testNegativeRegex() {
    Query query = new Query("SELECT MAX(test1) FROM foobar WHERE test1 !~ /[0-9]/;", DATABASE);
    Query select = select().max("test1").from(DATABASE, "foobar").where(nregex("test1", "/[0-9]/"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testInvalidNegativeRegex() {
    assertThrows(
        IllegalArgumentException.class,
        () -> select().max("test1").from(DATABASE, "foobar").where(nregex("test1", null)),
        "Missing text for expression");
  }

  @Test
  public void testContains() {
    Query query = new Query("SELECT MAX(test1) FROM foobar WHERE test1 =~ /text/;", DATABASE);
    Query select = select().max("test1").from(DATABASE, "foobar").where(contains("test1", "text"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testDistinct() {
    Query query = new Query("SELECT DISTINCT test1 FROM foobar;", DATABASE);
    Query select = select().column("test1").distinct().from(DATABASE, "foobar");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testDistinctWithExpression() {
    Query query =
        new Query("SELECT DISTINCT COUNT(test1) FROM foobar LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().count("test1").distinct().from(DATABASE, "foobar").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testDistinctWithMultipleSelectedColumns() {
    Query select =
        select().column("test1").column("test2").distinct().from(DATABASE, "foobar").limit(1, 20);

    assertThrows(
        IllegalStateException.class,
        () -> select.getCommand(),
        "DISTINCT function can only be used with one column");
  }

  @Test
  public void testDistinctWithoutSelectedColumns() {
    assertThrows(
        IllegalStateException.class,
        () -> select().distinct().from(DATABASE, "foobar").limit(1, 20),
        "DISTINCT function can only be used with one column");
  }

  @Test
  public void testMultipleColumns() {
    Query query = select().column("test1").distinct().column("test2").from(DATABASE, "foobar");
    assertThrows(
        IllegalStateException.class,
        () -> query.getCommand(),
        "Cannot mix all columns and specific columns");
  }

  @Test
  public void testNonEqual() {
    Query query = new Query("SELECT * FROM foobar WHERE test1 != 4;", DATABASE);
    Query select = select().all().from(DATABASE, "foobar").where(ne("test1", 4));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSelectAllWithColumn() {
    assertThrows(
        IllegalStateException.class,
        () ->
            select()
                .column("test1")
                .all()
                .from(DATABASE, "foobar")
                .where(ne("test1", raw("raw expression"))),
        "Can't select all columns over columns selected previously");
  }

  @Test
  public void testSelectAllWithColumns() {
    assertThrows(
        IllegalStateException.class,
        () ->
            select()
                .column("test1")
                .column("test2")
                .all()
                .from(DATABASE, "foobar")
                .where(ne("test1", raw("raw expression"))),
        "Can't select all columns over columns selected previously");
  }

  @Test
  public void testSelectAllWithDistinct() {
    assertThrows(
        IllegalStateException.class,
        () ->
            select()
                .column("test1")
                .distinct()
                .all()
                .from(DATABASE, "foobar")
                .where(ne("test1", raw("raw expression"))),
        "Can't select all columns over columns selected previously");
  }

  @Test
  public void testRawExpressionInWhere() {
    Query query = new Query("SELECT * FROM foobar WHERE test1 != raw expression;", DATABASE);
    Query select =
        select().all().from(DATABASE, "foobar").where(ne("test1", raw("raw expression")));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testRawExpressionEmptyValue() {
    String rawTextClause = null;
    assertThrows(
        IllegalArgumentException.class,
        () -> select().all().from(DATABASE, "foobar").where(rawTextClause),
        "Missing text for expression");
  }

  @Test
  public void testOrderingAsc() {
    Query query =
        new Query(
            "SELECT * FROM foobar WHERE test1 = 4 AND test2 > 'a' AND test2 <= 'z' ORDER BY time ASC;",
            DATABASE);
    Query select =
        select()
            .all()
            .from(DATABASE, "foobar")
            .where(eq("test1", 4))
            .and(gt("test2", "a"))
            .and(lte("test2", "z"))
            .orderBy(asc());

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testOrderingDesc() {
    Query query =
        new Query(
            "SELECT * FROM foobar WHERE test1 = 4 AND test2 > 'a' AND test2 <= 'z' ORDER BY time DESC;",
            DATABASE);
    Query select =
        select()
            .all()
            .from(DATABASE, "foobar")
            .where(eq("test1", 4))
            .and(gt("test2", "a"))
            .and(lte("test2", "z"))
            .orderBy(desc());

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSelect() {
    Query query =
        new Query("SELECT * FROM foobar WHERE test1 = 4 AND test2 > 'a' AND test2 <= 'z';", DATABASE);
    Query select =
        select()
            .all()
            .from(DATABASE, "foobar")
            .where(eq("test1", 4))
            .and(gt("test2", "a"))
            .and(lte("test2", "z"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSelectLtGte() {
    Query query = new Query("SELECT * FROM foobar WHERE test1 < 4 AND test2 >= 'a';", DATABASE);
    Query select =
        select().all().from(DATABASE, "foobar").where(lt("test1", 4)).and(gte("test2", "a"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testMean() {
    Query query =
        new Query(
            "SELECT MEAN(test1) FROM foobar WHERE test1 = 4 AND test2 > 'a' AND test2 <= 'z';", DATABASE);
    Query select =
        select()
            .mean("test1")
            .from(DATABASE, "foobar")
            .where(eq("test1", 4))
            .and(gt("test2", "a"))
            .and(lte("test2", "z"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSum() {
    Query query =
        new Query(
            "SELECT SUM(test1) FROM foobar WHERE test1 = 4 AND test2 > 'a' AND test2 <= 'z';", DATABASE);
    Query select =
        select()
            .sum("test1")
            .from(DATABASE, "foobar")
            .where(eq("test1", 4))
            .and(gt("test2", "a"))
            .and(lte("test2", "z"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testMin() {
    Query query =
        new Query(
            "SELECT MIN(test1) FROM foobar WHERE test1 = 4 AND test2 > 'a' AND test2 <= 'z';", DATABASE);
    Query select =
        select()
            .min("test1")
            .from(DATABASE, "foobar")
            .where(eq("test1", 4))
            .and(gt("test2", "a"))
            .and(lte("test2", "z"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testMax() {
    Query query =
        new Query(
            "SELECT MAX(test1) FROM foobar WHERE test1 = 4 AND test2 > 'a' AND test2 <= 'z';", DATABASE);
    Query select =
        select()
            .max("test1")
            .from(DATABASE, "foobar")
            .where(eq("test1", 4))
            .and(gt("test2", "a"))
            .and(lte("test2", "z"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSelectField() {
    Query query = new Query("SELECT test1,test2 FROM foobar;", DATABASE);
    Query select = select().column("test1").column("test2").from(DATABASE, "foobar");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testGroupBy() {
    Query query = new Query("SELECT test1 FROM foobar GROUP BY test2,test3;", DATABASE);
    Query select = select().column("test1").from(DATABASE, "foobar").groupBy("test2", "test3");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testGroupByTime() {
    Query query = new Query("SELECT test1 FROM foobar GROUP BY time(1h);", DATABASE);
    Query select = select().column("test1").from(DATABASE, "foobar").groupBy(time(1l, HOUR));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testGroupByTimeOffset() {
    Query query = new Query("SELECT test1 FROM foobar GROUP BY time(1h,5w);", DATABASE);
    Query select =
        select().column("test1").from(DATABASE, "foobar").groupBy(time(1l, HOUR, 5l, WEEK));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testGroupByTimeOffsetMultiples() {
    Query query = new Query("SELECT test1 FROM foobar GROUP BY time(1h,5w),test1;", DATABASE);
    Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .groupBy(time(1l, HOUR, 5l, WEEK), "test1");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testWhereConjunction() {
    Query query = new Query("SELECT test1 FROM foobar WHERE test1 = 1 OR test2 = 'a';", DATABASE);
    Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .where(eq("test1", 1))
            .or(eq("test2", "a"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testMultipleOrConjunction() {
    Query query =
        new Query("SELECT test1 FROM foobar WHERE test1 = 1 OR test2 = 'a' OR test3 = 'b';", DATABASE);
    Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .where(eq("test1", 1))
            .or(eq("test2", "a"))
            .or(eq("test3", "b"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testOrAndConjunction() {
    Query query =
        new Query(
            "SELECT test1 FROM foobar WHERE test1 = 1 OR test2 = 'a' OR test3 = 'b' AND test4 = 'c';",
            DATABASE);
    Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .where(eq("test1", 1))
            .or(eq("test2", "a"))
            .or(eq("test3", "b"))
            .and(eq("test4", "c"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testNestedClauses() {
    Query query =
        new Query(
            "SELECT test1 FROM foobar WHERE test1 = 1 OR test2 = 'a' OR test3 = 'b' "
                + "AND (test2 = 'b' OR test3 = 'a') "
                + "OR (test1 = 2 AND test2 = 'y' AND test3 = 'z') "
                + "AND (test1 = 8 OR test2 = 'g' OR test3 = 'j') "
                + "AND test4 = 'c';",
            DATABASE);
    Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .where(eq("test1", 1))
            .or(eq("test2", "a"))
            .or(eq("test3", "b"))
            .andNested()
            .and(eq("test2", "b"))
            .or(eq("test3", "a"))
            .close()
            .orNested()
            .and(eq("test1", 2))
            .and(eq("test2", "y"))
            .and(eq("test3", "z"))
            .close()
            .andNested()
            .or(eq("test1", 8))
            .or(eq("test2", "g"))
            .or(eq("test3", "j"))
            .close()
            .and(eq("test4", "c"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testWhere() {
    Query query = new Query("SELECT test1 FROM foobar WHERE test4 = 1;", DATABASE);
    Where where = select().column("test1").from(DATABASE, "foobar").where();
    Query select = where.and(eq("test4", 1));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testWhereGroupBy() {
    Query query =
        new Query("SELECT test1 FROM foobar WHERE test4 = 1 GROUP BY test2,test3;", DATABASE);
    Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .where(eq("test4", 1))
            .groupBy("test2", "test3");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testLimit() {
    Query query = new Query("SELECT test1 FROM foobar GROUP BY test2,test3 LIMIT 1;", DATABASE);
    Query select =
        select().column("test1").from(DATABASE, "foobar").groupBy("test2", "test3").limit(1);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testLimitTwice() {
    assertThrows(
        IllegalStateException.class,
        () ->
            select()
                .column("test1")
                .from(DATABASE, "foobar")
                .groupBy("test2", "test3")
                .limit(1)
                .limit(1),
        "Cannot use limit twice");
  }

  @Test
  public void testInvalidLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () -> select().column("test1").from(DATABASE, "foobar").groupBy("test2", "test3").limit(-1),
        "Invalid LIMIT value, must be strictly positive");
  }

  @Test
  public void testLimitOffset() {
    Query query =
        new Query("SELECT test1 FROM foobar GROUP BY test2,test3 LIMIT 1 OFFSET 20;", DATABASE);
    Query select =
        select().column("test1").from(DATABASE, "foobar").groupBy("test2", "test3").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSLimit() {
    Query query = new Query("SELECT test1 FROM foobar GROUP BY test2,test3 SLIMIT 1;", DATABASE);
    Query select =
        select().column("test1").from(DATABASE, "foobar").groupBy("test2", "test3").sLimit(1);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testInvalidSLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            select().column("test1").from(DATABASE, "foobar").groupBy("test2", "test3").sLimit(-1),
        "Invalid LIMIT value, must be strictly positive");
  }

  @Test
  public void testSLimitSOffset() {
    Query query =
        new Query("SELECT test1 FROM foobar GROUP BY test2,test3 SLIMIT 1 SOFFSET 20;", DATABASE);
    Query select =
        select().column("test1").from(DATABASE, "foobar").groupBy("test2", "test3").sLimit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testLimitAndSLimitOffset() {
    Query query =
        new Query(
            "SELECT test1 FROM foobar GROUP BY test2,test3 LIMIT 1 OFFSET 20 SLIMIT 1 SOFFSET 20;",
            DATABASE);
    Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .groupBy("test2", "test3")
            .limit(1, 20)
            .sLimit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testCount() {
    Query query = new Query("SELECT COUNT(test1) FROM foobar LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().count("test1").from(DATABASE, "foobar").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testTimezone() {
    Query query =
        new Query(
            "SELECT test1 FROM foobar GROUP BY test2,test3 SLIMIT 1 tz('America/Chicago');",
            DATABASE);
    Query select =
        select()
            .column("test1")
            .from(DATABASE, "foobar")
            .groupBy("test2", "test3")
            .sLimit(1)
            .tz("America/Chicago");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testMinWithLimit() {
    Query query = new Query("SELECT MIN(test1) FROM foobar LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().min("test1").from(DATABASE, "foobar").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testMaxWithLimit() {
    Query query = new Query("SELECT MAX(test1) FROM foobar LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().max("test1").from(DATABASE, "foobar").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSumWithLimit() {
    Query query = new Query("SELECT SUM(test1) FROM foobar LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().sum("test1").from(DATABASE, "foobar").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testAggregateCompination() {
    Query query =
        new Query(
            "SELECT MAX(test1),MIN(test2),COUNT(test3),SUM(test4) FROM foobar LIMIT 1 OFFSET 20;",
            DATABASE);
    Query select =
        select()
            .max("test1")
            .min("test2")
            .count("test3")
            .sum("test4")
            .from(DATABASE, "foobar")
            .limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testFunctionCall() {
    Query query = new Query("SELECT MEDIAN(test1) FROM foobar LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().function("MEDIAN", "test1").from(DATABASE, "foobar").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testFunctionInsideFunction() {
    Query query = new Query("SELECT MEDIAN(now()) FROM foobar LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().function("MEDIAN", now()).from(DATABASE, "foobar").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testRawTextOnSelection() {
    Query query =
        new Query("SELECT an expression on select FROM foobar LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().raw("an expression on select").from(DATABASE, "foobar").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testRawTextOnCondition() {
    Query query =
        new Query("SELECT * FROM foobar WHERE text as condition LIMIT 1 OFFSET 20;", DATABASE);
    Query select = select().from(DATABASE, "foobar").where("text as condition").limit(1, 20);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testNowOnCondition() {
    Query query = new Query("SELECT * FROM foobar WHERE time > now() AND time <= now();", DATABASE);
    Query select =
        select().from(DATABASE, "foobar").where(gt("time", now())).and(lte("time", now()));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSelectRegex() {
    Query query = new Query("SELECT /k/ FROM foobar;", DATABASE);
    Query select = select().regex("/k/").from(DATABASE, "foobar");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testCountAll() {
    Query query = new Query("SELECT COUNT(*) FROM foobar;", DATABASE);
    Query select = select().countAll().from(DATABASE, "foobar");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testCountAllWithColumn() {
    assertThrows(
        IllegalStateException.class,
        () -> select().column("test1").countAll().from(DATABASE, "foobar"),
        "Can't count all with previously selected columns");
  }

  @Test
  public void testCountAllWithColumns() {
    assertThrows(
        IllegalStateException.class,
        () -> select().column("test1").column("test2").countAll().from(DATABASE, "foobar"),
        "Can't count all with previously selected columns");
  }

  @Test
  public void testRequiresPost() {
    Query select = select().requiresPost().countAll().from(DATABASE, "foobar");
    Query selectColumns = select("column1", "column2").requiresPost().from(DATABASE, "foobar");
    Query selectColumnsAndAggregations =
        select(min("column1"), max("column2")).requiresPost().from(DATABASE, "foobar");

    assertTrue(select.requiresPost());
    assertTrue(selectColumns.requiresPost());
    assertTrue(selectColumnsAndAggregations.requiresPost());
  }

  @Test
  public void testInto() {
    Query query =
        new Query(
            "SELECT * INTO \"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT FROM \"NOAA_water_database\".\"autogen\"./.*/ GROUP BY *;",
            DATABASE);
    Query select =
        select()
            .into("\"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT")
            .from(DATABASE, "\"NOAA_water_database\".\"autogen\"./.*/")
            .groupBy(new RawText("*"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testIntoWithSelection() {
    Query query =
        new Query(
            "SELECT column1,MAX(column2),MAX(column3) INTO \"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT FROM \"NOAA_water_database\".\"autogen\"./.*/ GROUP BY *;",
            DATABASE);
    Query select =
        select()
            .column("column1")
            .max("column2")
            .max("column3")
            .into("\"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT")
            .from(DATABASE, "\"NOAA_water_database\".\"autogen\"./.*/")
            .groupBy(new RawText("*"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubRelativeTimeSelection() {
    Query query = new Query("SELECT water_level FROM h2o_feet WHERE time > now() - 1h;", DATABASE);
    Query select =
        select()
            .column("water_level")
            .from(DATABASE, "h2o_feet")
            .where(gt("time", subTime(1l, HOUR)));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testAddRelativeTimeSelection() {
    Query query = new Query("SELECT water_level FROM h2o_feet WHERE time > now() + 1w;", DATABASE);
    Query select =
        select()
            .column("water_level")
            .from(DATABASE, "h2o_feet")
            .where(gt("time", addTime(1l, WEEK)));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testDateTimeString() {
    Query query =
        new Query(
            "SELECT water_level FROM h2o_feet WHERE time > '2015-08-18T00:00:00.000000000Z';",
            DATABASE);
    Query select =
        select()
            .column("water_level")
            .from(DATABASE, "h2o_feet")
            .where(gt("time", "2015-08-18T00:00:00.000000000Z"));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testDateTimeStringOperation() {
    Query query =
        new Query(
            "SELECT water_level FROM h2o_feet WHERE time > '2015-09-18T21:24:00Z' + 6m;", DATABASE);
    Query select =
        select()
            .column("water_level")
            .from(DATABASE, "h2o_feet")
            .where(gt("time", op("2015-09-18T21:24:00Z", ADD, ti(6l, MINUTE))));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testDateTimeEpochOperation() {
    Query query = new Query("SELECT water_level FROM h2o_feet WHERE time > 24043524m - 6m;", DATABASE);
    Query select =
        select()
            .column("water_level")
            .from(DATABASE, "h2o_feet")
            .where(gt("time", op(ti(24043524l, MINUTE), SUB, ti(6l, MINUTE))));

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

    @Test
    public void testFill() {
        Query query = new Query("SELECT water_level FROM h2o_feet WHERE time > 24043524m - 6m GROUP BY water_level fill(100);", DATABASE);
        Query select =
                select()
                        .column("water_level")
                        .from(DATABASE, "h2o_feet")
                        .where(gt("time", op(ti(24043524l, MINUTE), SUB, ti(6l, MINUTE))))
                        .groupBy("water_level")
                        .fill(100);

        assertEquals(query.getCommand(), select.getCommand());
        assertEquals(query.getDatabase(), select.getDatabase());
    }

  @Test
  public void testFillLinear() {
    Query query = new Query("SELECT water_level FROM h2o_feet WHERE time > 24043524m - 6m GROUP BY water_level fill(linear);", DATABASE);
    Query select =
            select()
                    .column("water_level")
                    .from(DATABASE, "h2o_feet")
                    .where(gt("time", op(ti(24043524l, MINUTE), SUB, ti(6l, MINUTE))))
                    .groupBy("water_level")
                    .fill("linear");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }
}

package org.influxdb.querybuilder;

import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.*;
import static org.influxdb.querybuilder.time.DurationLiteral.HOUR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.influxdb.dto.Query;
import org.junit.jupiter.api.Test;

public class SelectionSubQueryImplTest {

  private static final String DATABASE = "testdb";

  @Test
  public void testSubQueryWithoutTable() {
    String[] tables = null;
    assertThrows(
        IllegalArgumentException.class,
        () -> select().max("test1").as("hello").fromSubQuery(DATABASE).from(tables).close());
  }

  @Test
  public void testSubQuery() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT * FROM foobar) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE, "foobar")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryMultipleTables() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT * FROM foobar,second_table) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE, new String[] {"foobar", "second_table"})
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryRawTable() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT * FROM /*/) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQueryRaw(DATABASE, "/*/")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryWithTextOnWhere() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT * FROM foobar WHERE arbitrary text) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE, "foobar")
            .where("arbitrary text")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryWithLimit() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT * FROM foobar LIMIT 1) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE, "foobar")
            .limit(1)
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryWhereNestedOrderByLimit() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT * FROM foobar WHERE (test1 = 2) AND test1 = 1 ORDER BY time ASC LIMIT 1 OFFSET 1 SLIMIT 1 SOFFSET 1) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE, "foobar")
            .where()
            .orNested()
            .or(eq("test1", 2))
            .close()
            .where(eq("test1", 1))
            .orderBy(asc())
            .limit(1)
            .limit(1, 1)
            .sLimit(1)
            .sLimit(1, 1)
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryWithLimitOFFSET() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT * FROM foobar LIMIT 1 OFFSET 2) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE, "foobar")
            .limit(1, 2)
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryCountAll() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT COUNT(*) FROM foobar) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .countAll()
            .from("foobar")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryWithTables() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT COUNT(*) FROM foobar,foobar2) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .countAll()
            .from(new String[] {"foobar", "foobar2"})
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryWithRawString() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT COUNT(*) FROM /*/) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .countAll()
            .fromRaw("/*/")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryAs() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT column1 AS newname FROM foobar) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .column("column1")
            .as("newname")
            .from("foobar")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryColumns() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT column1,column2 FROM foobar) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .column("column1")
            .column("column2")
            .from("foobar")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  /*test * */

  @Test
  public void testSubQueryWithSubQueries() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM ("
                + "SELECT MAX(column1),MAX(column2) FROM ("
                + "SELECT * FROM ("
                + "SELECT MAX(column1),MEAN(column2) FROM ("
                + "SELECT DISTINCT test1 FROM foobar WHERE column1 > 3 GROUP BY column2"
                + ")"
                + ") WHERE column1 = 5 GROUP BY column2 LIMIT 50 OFFSET 10"
                + ") WHERE column1 = 4"
                + ") WHERE column3 = 5 LIMIT 5 OFFSET 10;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .max("column1")
            .max("column2")
            .fromSubQuery()
            .all()
            .fromSubQuery()
            .max("column1")
            .mean("column2")
            .fromSubQuery()
            .column("test1")
            .distinct()
            .from("foobar")
            .where(gt("column1", 3))
            .groupBy("column2")
            .close()
            .close()
            .where(eq("column1", 5))
            .groupBy("column2")
            .limit(50, 10)
            .close()
            .where(eq("column1", 4))
            .close()
            .where(eq("column3", 5))
            .limit(5, 10);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryWithLimitAndSOffSET() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM ("
                + "SELECT MAX(column1),MAX(column2) FROM ("
                + "SELECT * FROM ("
                + "SELECT MAX(column1),MEAN(column2) FROM ("
                + "SELECT DISTINCT test1 FROM foobar WHERE column1 > 3 GROUP BY column2 LIMIT 1 OFFSET 20 SLIMIT 2 SOFFSET 10"
                + ")"
                + ") WHERE column1 = 5 GROUP BY column2 ORDER BY time DESC LIMIT 50 OFFSET 10"
                + ") WHERE column1 = 4 OR column1 = 7 GROUP BY time(4h) ORDER BY time ASC SLIMIT 3"
                + ") WHERE column3 = 5 LIMIT 5 OFFSET 10;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .max("column1")
            .max("column2")
            .fromSubQuery()
            .all()
            .fromSubQuery()
            .max("column1")
            .mean("column2")
            .fromSubQuery()
            .column("test1")
            .distinct()
            .from("foobar")
            .where(gt("column1", 3))
            .groupBy("column2")
            .limit(1, 20)
            .sLimit(2, 10)
            .close()
            .close()
            .where(eq("column1", 5))
            .groupBy("column2")
            .limit(50, 10)
            .orderBy(desc())
            .close()
            .where(eq("column1", 4))
            .or(eq("column1", 7))
            .sLimit(3)
            .orderBy(asc())
            .groupBy(time(4l, HOUR))
            .close()
            .where(eq("column3", 5))
            .limit(5, 10);

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryTimeZoneColumns() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT column1,column2 FROM foobar tz('America/Chicago')) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .column("column1")
            .column("column2")
            .from("foobar")
            .tz("America/Chicago")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQuerySelectRegexColumns() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT /k/ FROM foobar tz('America/Chicago')) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .regex("/k/")
            .from("foobar")
            .tz("America/Chicago")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }

  @Test
  public void testSubQueryNested() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT /k/ FROM foobar WHERE (column1 = 2 OR column1 = 3) OR (column2 = 5 AND column2 = 7) tz('America/Chicago')) WHERE column1 = 1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .regex("/k/")
            .from("foobar")
            .where()
            .andNested()
            .and(eq("column1", 2))
            .or(eq("column1", 3))
            .close()
            .orNested()
            .and(eq("column2", 5))
            .and(eq("column2", 7))
            .close()
            .tz("America/Chicago")
            .close()
            .where(eq("column1", 1))
            .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }
}

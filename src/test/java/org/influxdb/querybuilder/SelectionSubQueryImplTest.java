package org.influxdb.querybuilder;

import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.*;
import static org.influxdb.querybuilder.time.DurationLiteral.HOUR;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.influxdb.dto.Query;
import org.junit.jupiter.api.Test;

public class SelectionSubQueryImplTest {

  private static final String DATABASE = "testdb";

  @Test
  public void testSubQuery() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT * FROM foobar) WHERE column1=1 GROUP BY time;",
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
  public void testSubQueryColumns() {
    Query query =
        new Query(
            "SELECT column1,column2 FROM (SELECT column1,column2 FROM foobar) WHERE column1=1 GROUP BY time;",
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
                + "SELECT DISTINCT test1 FROM foobar WHERE column1>3 GROUP BY column2"
                + ")"
                + ") WHERE column1=5 GROUP BY column2 LIMIT 50 OFFSET 10"
                + ") WHERE column1=4"
                + ") WHERE column3=5 LIMIT 5 OFFSET 10;",
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
                + "SELECT DISTINCT test1 FROM foobar WHERE column1>3 GROUP BY column2 LIMIT 1 OFFSET 20 SLIMIT 2 SOFFSET 10"
                + ")"
                + ") WHERE column1=5 GROUP BY column2 LIMIT 50 OFFSET 10"
                + ") WHERE column1=4 OR column1=7 GROUP BY time(4h) SLIMIT 3"
                + ") WHERE column3=5 LIMIT 5 OFFSET 10;",
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
            .or(eq("column1",7))
            .sLimit(3)
            .orderBy(asc())
            .groupBy(time(4l,HOUR))
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
            "SELECT column1,column2 FROM (SELECT column1,column2 FROM foobar tz('America/Chicago')) WHERE column1=1 GROUP BY time;",
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
            "SELECT column1,column2 FROM (SELECT /k/ FROM foobar tz('America/Chicago')) WHERE column1=1 GROUP BY time;",
            DATABASE);
    Query select =
        select()
            .requiresPost()
            .column("column1")
            .column("column2")
            .fromSubQuery(DATABASE)
            .regex("k")
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
                    "SELECT column1,column2 FROM (SELECT /k/ FROM foobar WHERE (column1=2 OR column1=3) OR (column2=5 AND column2=7) tz('America/Chicago')) WHERE column1=1 GROUP BY time;",
                    DATABASE);
    Query select =
            select()
                    .requiresPost()
                    .column("column1")
                    .column("column2")
                    .fromSubQuery(DATABASE)
                    .regex("k")
                    .from("foobar")
                    .where()
                    .andNested()
                    .and(eq("column1",2))
                    .or(eq("column1",3))
                    .close()
                    .orNested()
                    .and(eq("column2",5))
                    .and(eq("column2",7))
                    .close()
                    .tz("America/Chicago")
                    .close()
                    .where(eq("column1", 1))
                    .groupBy("time");

    assertEquals(query.getCommand(), select.getCommand());
    assertEquals(query.getDatabase(), select.getDatabase());
  }
}

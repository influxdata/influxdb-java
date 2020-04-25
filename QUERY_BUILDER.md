# QueryBuilder

Supposing that you have a measurement _h2o_feet_:

```sqlite-psql
> SELECT * FROM "h2o_feet"

name: h2o_feet
--------------
time                   level description      location       water_level
2015-08-18T00:00:00Z   below 3 feet           santa_monica   2.064
2015-08-18T00:00:00Z   between 6 and 9 feet   coyote_creek   8.12
[...]
2015-09-18T21:36:00Z   between 3 and 6 feet   santa_monica   5.066
2015-09-18T21:42:00Z   between 3 and 6 feet   santa_monica   4.938
```

## The basic SELECT statement

Issue simple select statements

```java
Query query = select().from(DATABASE,"h2o_feet");
```

```sqlite-psql
SELECT * FROM "h2o_feet"
```

Select specific tags and fields from a single measurement

```java
Query query = select("level description","location","water_level").from(DATABASE,"h2o_feet");
```

```sqlite-psql
SELECT "level description",location,water_level FROM h2o_feet;
```

Select specific tags and fields from a single measurement, and provide their identifier type

```java
Query query = select().column("\"level description\"::field").column("\"location\"::tag").column("\"water_level\"::field").from(DATABASE,"h2o_feet");
```

```sqlite-psql
SELECT "level description"::field,"location"::tag,"water_level"::field FROM h2o_feet;
```

Select all fields from a single measurement

```java
Query query = select().raw("*::field").from(DATABASE,"h2o_feet");
```

```sqlite-psql
SELECT *::field FROM h2o_feet;
```

Select a specific field from a measurement and perform basic arithmetic

```java
Query query = select().op(op(cop("water_level",MUL,2),"+",4)).from(DATABASE,"h2o_feet");
```

```sqlite-psql
SELECT (water_level * 2) + 4 FROM h2o_feet;
```

Select all data from more than one measurement

```java
Query query = select().from(DATABASE,"\"h2o_feet\",\"h2o_pH\"");
```

```sqlite-psql
SELECT * FROM "h2o_feet","h2o_pH";
```

Select all data from a fully qualified measurement

```java
Query query = select().from(DATABASE,"\"NOAA_water_database\".\"autogen\".\"h2o_feet\"");
```

```sqlite-psql
SELECT * FROM "NOAA_water_database"."autogen"."h2o_feet";
```

Select data that have specific field key-values

```java
Query query = select().from(DATABASE,"h2o_feet").where(gt("water_level",8));
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE water_level > 8;
```

Select data that have a specific string field key-value

```java
Query query = select().from(DATABASE,"h2o_feet").where(eq("level description","below 3 feet"));
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE "level description" = 'below 3 feet';
```

Select data that have a specific field key-value and perform basic arithmetic

```java
Query query = select().from(DATABASE,"h2o_feet").where(gt(cop("water_level",ADD,2),11.9));
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE (water_level + 2) > 11.9;
```

Select data that have a specific tag key-value

```java
Query query = select().column("water_level").from(DATABASE,"h2o_feet").where(eq("location","santa_monica"));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica';
```

Select data that have specific field key-values and tag key-values

```java
Query query = select().column("water_level").from(DATABASE,"h2o_feet")
                .where(neq("location","santa_monica"))
                .andNested()
                .and(lt("water_level",-0.59))
                .or(gt("water_level",9.95))
                .close();
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location <> 'santa_monica' AND (water_level < -0.59 OR water_level > 9.95);
```

Select data that have specific timestamps

```java
Query query = select().from(DATABASE,"h2o_feet")
                .where(gt("time",subTime(7,DAY)));
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE time > now() - 7d;
```

## The GROUP BY clause

Group query results by a single tag

```java
Query query = select().mean("water_level").from(DATABASE,"h2o_feet") .groupBy("location");
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet GROUP BY location;
```

Group query results by more than one tag

```java
Query query = select().mean("index").from(DATABASE,"h2o_feet")
                .groupBy("location","randtag");
```

```sqlite-psql
SELECT MEAN(index) FROM h2o_feet GROUP BY location,randtag;
```

Group query results by all tags

```java
Query query = select().mean("index").from(DATABASE,"h2o_feet")
                .groupBy(raw("*"));
```

```sqlite-psql
SELECT MEAN(index) FROM h2o_feet GROUP BY *;
```

## GROUP BY time interval

Group query results into 12 minute intervals

```java
Query query = select().count("water_level").from(DATABASE,"h2o_feet")
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:30:00Z'"))
                .groupBy(time(12l,MINUTE));
```

```sqlite-psql
SELECT COUNT(water_level) FROM h2o_feet WHERE location = 'coyote_creek' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z'' GROUP BY time(12m);
```

Group query results into 12 minutes intervals and by a tag key

```java
        Query query = select().count("water_level").from(DATABASE,"h2o_feet")
                .where()
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:30:00Z'"))
                .groupBy(time(12l,MINUTE),"location");
```

```sqlite-psql
SELECT COUNT(water_level) FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z'' GROUP BY time(12m),location;
```

## Advanced GROUP BY time() syntax

Group query results into 18 minute intervals and shift the preset time boundaries forward

```java
Query query = select().mean("water_level").from(DATABASE,"h2o_feet")
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:06:00Z"))
                .and(lte("time","2015-08-18T00:54:00Z"))
                .groupBy(time(18l,MINUTE,6l,MINUTE));
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE location = 'coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m,6m);
```

Group query results into 12 minute intervals and shift the preset time boundaries back

```java
Query query = select().mean("water_level").from(DATABASE,"h2o_feet")
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:06:00Z"))
                .and(lte("time","2015-08-18T00:54:00Z"))
                .groupBy(time(18l,MINUTE,-12l,MINUTE));
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE location = 'coyote_creek' AND time >= '2015-08-18T00:06:00Z' AND time <= '2015-08-18T00:54:00Z' GROUP BY time(18m,-12m);
```

## GROUP BY time intervals and fill()

```java
Query select = select()
                .column("water_level")
                .from(DATABASE, "h2o_feet")
                .where(gt("time", op(ti(24043524l, MINUTE), SUB, ti(6l, MINUTE))))
                .groupBy("water_level")
                .fill(100);
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE time > 24043524m - 6m GROUP BY water_level fill(100);"
```

## The INTO clause

Rename a database

```java
Query select = select()
                .into("\"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT")
                .from(DATABASE, "\"NOAA_water_database\".\"autogen\"./.*/")
                .groupBy(new RawText("*"));
```

```sqlite-psql
SELECT * INTO "copy_NOAA_water_database"."autogen".:MEASUREMENT FROM "NOAA_water_database"."autogen"./.*/ GROUP BY *;
```

Write the results of a query to a measurement

```java
Query select = select().column("water_level").into("h2o_feet_copy_1").from(DATABASE,"h2o_feet").where(eq("location","coyote_creek"));
```

```sqlite-psql
SELECT water_level INTO h2o_feet_copy_1 FROM h2o_feet WHERE location = 'coyote_creek';
```

Write aggregated results to a measurement

```java
Query select = select()
                .mean("water_level")
                .into("all_my_averages")
                .from(DATABASE,"h2o_feet")
                .where(eq("location","coyote_creek"))
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:30:00Z"))
                .groupBy(time(12l,MINUTE));
```

```sqlite-psql
SELECT MEAN(water_level) INTO all_my_averages FROM h2o_feet WHERE location = 'coyote_creek' AND time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m);
```

Write aggregated results for more than one measurement to a different database (downsampling with backreferencing)

```java
Query select = select()
                .mean(raw("*"))
                .into("\"where_else\".\"autogen\".:MEASUREMENT")
                .fromRaw(DATABASE, "/.*/")
                .where(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:06:00Z"))
                    .groupBy(time(12l,MINUTE));
```

```sqlite-psql
SELECT MEAN(*) INTO "where_else"."autogen".:MEASUREMENT FROM /.*/ WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:06:00Z' GROUP BY time(12m);
```

## ORDER BY time DESC

Return the newest points first

```java
Query select = select().from(DATABASE,"h2o_feet")
                .where(eq("location","santa_monica"))
                .orderBy(desc());
```

```sqlite-psql
SELECT * FROM h2o_feet WHERE location = 'santa_monica' ORDER BY time DESC;
```

Return the newest points first and include a GROUP BY time() clause

```java
Query select = select().mean("water_level")
                .from(DATABASE,"h2o_feet")
                .where(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:42:00Z"))
                .groupBy(time(12l,MINUTE))
                .orderBy(desc());
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY time(12m) ORDER BY time DESC;
```

## The LIMIT clause

Limit the number of points returned

```java
Query select = select("water_level","location")
                .from(DATABASE,"h2o_feet").limit(3);
```

```sqlite-psql
SELECT water_level,location FROM h2o_feet LIMIT 3;
```

Limit the number points returned and include a GROUP BY clause

```java
Query select = select().mean("water_level")
                .from(DATABASE,"h2o_feet")
                .where()
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:42:00Z"))
                .groupBy(raw("*"),time(12l,MINUTE))
                .limit(2);
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) LIMIT 2;
```

## The SLIMIT clause

Limit the number of series returned

```java
Query select = select().column("water_level")
                .from(DATABASE,"h2o_fleet")
                .groupBy(raw("*"))
                .sLimit(1);
```

```sqlite-psql
SELECT water_level FROM "h2o_feet" GROUP BY * SLIMIT 1
```

Limit the number of series returned and include a GROUP BY time() clause

```java
Query select = select().column("water_level")
                .from(DATABASE,"h2o_feet")
                .where()
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:42:00Z"))
                .groupBy(raw("*"),time(12l,MINUTE))
                .sLimit(1);
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) SLIMIT 1;
```

## The OFFSET clause

Paginate points

```java
Query select = select("water_level","location").from(DATABASE,"h2o_feet").limit(3,3);
```

```sqlite-psql
SELECT water_level,location FROM h2o_feet LIMIT 3 OFFSET 3;
```

## The SOFFSET clause

Paginate series and include all clauses

```java
Query select = select().mean("water_level")
                .from(DATABASE,"h2o_feet")
                .where()
                .and(gte("time","2015-08-18T00:00:00Z"))
                .and(lte("time","2015-08-18T00:42:00Z"))
                .groupBy(raw("*"),time(12l,MINUTE))
                .orderBy(desc())
                .limit(2,2)
                .sLimit(1,1);
```

```sqlite-psql
SELECT MEAN(water_level) FROM h2o_feet WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:42:00Z' GROUP BY *,time(12m) ORDER BY time DESC LIMIT 2 OFFSET 2 SLIMIT 1 SOFFSET 1;
```

## The Time Zone clause

Return the UTC offset for Chicago’s time zone

```java
Query select = select()
                .column("test1")
                .from(DATABASE, "h2o_feet")
                .groupBy("test2", "test3")
                .sLimit(1)
                .tz("America/Chicago");
```

```sqlite-psql
SELECT test1 FROM foobar GROUP BY test2,test3 SLIMIT 1 tz('America/Chicago');
```

## Time Syntax

Specify a time range with RFC3339 date-time strings

```java
Query select = select().column("water_level")
                .from(DATABASE,"h2o_feet")
                .where(eq("location","santa_monica"))
                .and(gte("time","2015-08-18T00:00:00.000000000Z"))
                .and(lte("time","2015-08-18T00:12:00Z"));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= '2015-08-18T00:00:00.000000000Z' AND time <= '2015-08-18T00:12:00Z';
```

Specify a time range with second-precision epoch timestamps

```java
Query select = select().column("water_level")
                .from(DATABASE,"h2o_feet")
                .where(eq("location","santa_monica"))
                .and(gte("time",ti(1439856000l,SECOND)))
                .and(lte("time",ti(1439856720l,SECOND)));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= 1439856000s AND time <= 1439856720s;
```

Perform basic arithmetic on an RFC3339-like date-time string

```java
Query select = select().column("water_level")
                .from(DATABASE,"h2o_feet")
                .where(eq("location","santa_monica"))
                .and(gte("time",op("2015-09-18T21:24:00Z",SUB,ti(6l,MINUTE))));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= '2015-09-18T21:24:00Z' - 6m;
```

Perform basic arithmetic on an epoch timestamp

```java
Query select = select().column("water_level")
                .from(DATABASE,"h2o_feet")
                .where(eq("location","santa_monica"))
                .and(gte("time",op(ti(24043524l,MINUTE),SUB,ti(6l,MINUTE))));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= 24043524m - 6m;
```

Specify a time range with relative time

```java
Query select = select().column("water_level")
                .from(DATABASE,"h2o_feet")
                .where(eq("location","santa_monica"))
                .and(gte("time",subTime(1l,HOUR)));
```

```sqlite-psql
SELECT water_level FROM h2o_feet WHERE location = 'santa_monica' AND time >= now() - 1h;
```

## Regular expressions

Use a regular expression to specify field keys and tag keys in the SELECT clause

```java
Query select = select().regex("l").from(DATABASE,"h2o_feet").limit(1);
```

```sqlite-psql
SELECT /l/ FROM h2o_feet LIMIT 1;
```

Use a regular expression to specify field keys with a function in the SELECT clause

```java
Query select = select().regex("l").distinct().from(DATABASE,"h2o_feet").limit(1);
```

```sqlite-psql
SELECT DISTINCT /l/ FROM h2o_feet LIMIT 1;
```

Use a regular expression to specify measurements in the FROM clause

```java
Query select = select().mean("degrees").fromRaw(DATABASE,"/temperature/");
```

```sqlite-psql
SELECT MEAN(degrees) FROM /temperature/;
```

Use a regular expression to specify a field value in the WHERE clause

```java
Query select = select().regex("/l/").from(DATABASE,"h2o_feet").where(regex("level description","/between/")).limit(1);
```

```sqlite-psql
SELECT /l/ FROM h2o_feet WHERE "level description" =~ /between/ LIMIT 1;
```

Use a regular expression to specify tag keys in the GROUP BY clause

```java
Query select = select().regex("/l/").from(DATABASE,"h2o_feet").where(regex("level description","/between/")).groupBy(raw("/l/")).limit(1);
```

```sqlite-psql
SELECT /l/ FROM h2o_feet WHERE "level description" =~ /between/ GROUP BY /l/ LIMIT 1;
```

Function with no direct implementation can be supported by raw expressions

```java
Query select = select().raw("an expression on select").from(dbName, "cpu").where("an expression as condition");
```

```sqlite-psql
SELECT an expression on select FROM h2o_feet WHERE an expression as condition;
```

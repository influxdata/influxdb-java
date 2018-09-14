package org.influxdb.querybuilder;

import static org.influxdb.querybuilder.Operations.EQ;
import static org.influxdb.querybuilder.Operations.GT;
import static org.influxdb.querybuilder.Operations.GTE;
import static org.influxdb.querybuilder.Operations.LT;
import static org.influxdb.querybuilder.Operations.LTE;
import static org.influxdb.querybuilder.Operations.NE;

import java.util.Arrays;
import org.influxdb.dto.Query;
import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.ContainsClause;
import org.influxdb.querybuilder.clauses.NegativeRegexClause;
import org.influxdb.querybuilder.clauses.RegexClause;
import org.influxdb.querybuilder.clauses.SimpleClause;

public abstract class BuiltQuery extends Query implements QueryStringBuilder {

  public BuiltQuery(final String database) {
    super(null, database);
  }

  public BuiltQuery(final String database, final boolean requiresPost) {
    super(null, database, requiresPost);
  }

  static StringBuilder addSemicolonIfMissing(final StringBuilder stringBuilder) {
    int length = trimLast(stringBuilder);
    if (length == 0 || stringBuilder.charAt(length - 1) != ';') {
      stringBuilder.append(';');
    }
    return stringBuilder;
  }

  static int trimLast(final StringBuilder stringBuilder) {
    int length = stringBuilder.length();
    while (length > 0 && stringBuilder.charAt(length - 1) <= ' ') {
      length -= 1;
    }
    if (length != stringBuilder.length()) {
      stringBuilder.setLength(length);
    }
    return length;
  }

  @Override
  public String getCommand() {
    StringBuilder sb = buildQueryString();
    addSemicolonIfMissing(sb);
    return sb.toString();
  }

  @Override
  public String getCommandWithUrlEncoded() {
    return encode(getCommand());
  }

  /**
   * The query builder shall provide all the building blocks needed, only a static block shall be
   * used.
   */
  public static final class QueryBuilder {

    private QueryBuilder() {
    }

    public static SelectionQueryImpl select(final String... columns) {
      return select((Object[]) columns);
    }

    public static SelectionQueryImpl select(final Object... columns) {
      WhereCoreImpl whereCore = new WhereCoreImpl();
      SelectCoreImpl selectCore =
          new SelectCoreImpl(null, Arrays.asList(columns), false, whereCore);
      whereCore.setStatement(selectCore);
      return new SelectionQueryImpl(new SelectionCoreImpl());
    }

    public static Clause eq(final String name, final Object value) {
      return new SimpleClause(name, EQ, value);
    }

    public static Clause ne(final String name, final Object value) {
      return new SimpleClause(name, NE, value);
    }

    public static Clause contains(final String name, final String value) {
      return new ContainsClause(name, value);
    }

    public static Clause regex(final String name, final String value) {
      return new RegexClause(name, value);
    }

    public static Clause nregex(final String name, final String value) {
      return new NegativeRegexClause(name, value);
    }

    public static Clause lt(final String name, final Object value) {
      return new SimpleClause(name, LT, value);
    }

    public static Clause lte(final String name, final Object value) {
      return new SimpleClause(name, LTE, value);
    }

    public static Clause gt(final String name, final Object value) {
      return new SimpleClause(name, GT, value);
    }

    public static Clause gte(final String name, final Object value) {
      return new SimpleClause(name, GTE, value);
    }

    /** @return */
    public static Ordering asc() {
      return new Ordering(false);
    }

    /**
     * InfluxDB supports only time for ordering.
     *
     * @return
     */
    public static Ordering desc() {
      return new Ordering(true);
    }

    public static Object raw(final String str) {
      return new RawText(str);
    }

    public static Object max(final Object column) {
      return FunctionFactory.max(column);
    }

    public static Object min(final Object column) {
      return FunctionFactory.min(column);
    }

    public static Object time(Long timeInterval, String durationLiteral) {
      return FunctionFactory.time(timeInterval, durationLiteral);
    }

    public static Object time(
        Long timeInterval, String durationLiteral, Long offsetInterval, String offSetLiteral) {
      return FunctionFactory.time(timeInterval, durationLiteral, offsetInterval, offSetLiteral);
    }

    public static Object now() {
      return FunctionFactory.now();
    }
  }
}

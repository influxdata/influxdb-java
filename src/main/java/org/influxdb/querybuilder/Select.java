package org.influxdb.querybuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.influxdb.querybuilder.clauses.Clause;
import org.influxdb.querybuilder.clauses.RawTextClause;

public class Select extends BuiltQuery {

  private final String table;
  private final boolean isDistinct;
  private final List<Object> columns;
  private final Where where;
  private Ordering ordering;
  private List<Object> groupByColumns;
  private Integer limit;
  private Long offSet;

  Select(
      final String database,
      final String table,
      final List<Object> columns,
      final boolean isDistinct,
      final boolean requiresPost) {
    super(database, requiresPost);
    this.table = table;
    this.columns = columns;
    this.isDistinct = isDistinct;
    this.where = new Where(this);
  }

  @Override
  StringBuilder buildQueryString() {
    StringBuilder builder = new StringBuilder();

    builder.append("SELECT ");

    if (isDistinct) {
      if (columns.size() > 1) {
        throw new IllegalStateException("DISTINCT function can only be used with one column");
      }
    }

    if (columns == null) {
      builder.append('*');
    } else {
      Appender.joinAndAppendNames(builder, columns);
    }
    builder.append(" FROM ");

    Appender.appendName(table, builder);

    if (!where.clauses.isEmpty()) {
      builder.append(" WHERE ");
      Appender.joinAndAppend(builder, " AND ", where.clauses);
    }

    if (groupByColumns != null) {
      builder.append(" GROUP BY ");
      Appender.joinAndAppendNames(builder, groupByColumns);
    }

    if (ordering != null) {
      builder.append(" ORDER BY ");
      Appender.joinAndAppend(builder, ",", Collections.singletonList(ordering));
    }

    if (limit != null) {
      builder.append(" LIMIT ").append(limit);
    }

    if (offSet != null) {
      builder.append(" OFFSET ").append(offSet);
    }

    return builder;
  }

  public Where where(final Clause clause) {
    return where.and(clause);
  }

  public Where where(final String text) {
    return where.and(new RawTextClause(text));
  }

  public Select orderBy(final Ordering ordering) {

    this.ordering = ordering;
    return this;
  }

  public Select groupBy(final Object... columns) {
    this.groupByColumns = Arrays.asList(columns);
    return this;
  }

  public Select limit(final int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("Invalid LIMIT value, must be strictly positive");
    }

    if (this.limit != null) {
      throw new IllegalStateException("A LIMIT value has already been provided");
    }

    this.limit = limit;
    return this;
  }

  public Select limit(final int limit, final long offSet) {
    if (limit <= 0 || offSet <= 0) {
      throw new IllegalArgumentException(
          "Invalid LIMIT and OFFSET Value, must be strictly positive");
    }

    this.limit = limit;
    this.offSet = offSet;
    return this;
  }

  public static class Where extends BuiltQueryDecorator<Select> {

    private final List<Clause> clauses = new ArrayList<Clause>();

    Where(final Select statement) {
      super(statement);
    }

    public Where and(final Clause clause) {
      clauses.add(clause);
      return this;
    }

    public Select orderBy(final Ordering orderings) {
      return query.orderBy(orderings);
    }

    public Select groupBy(final Object... columns) {
      return query.groupBy(columns);
    }

    public Select limit(final int limit) {
      return query.limit(limit);
    }

    public Select limit(final int limit, final long offSet) {
      return query.limit(limit, offSet);
    }
  }

  public static class Builder {

    protected List<Object> columns;
    protected boolean requiresPost;
    protected boolean isDistinct;

    Builder() {
    }

    public Builder(final List<Object> columns) {
      this.columns = columns;
    }

    public Builder requiresPost() {
      this.requiresPost = true;
      return this;
    }

    public Select from(final String table) {
      return from(null, table);
    }

    public Select from(final String database, final String table) {
      return new Select(database, table, columns, isDistinct, requiresPost);
    }
  }
}

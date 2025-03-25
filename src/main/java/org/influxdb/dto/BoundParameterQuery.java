package org.influxdb.dto;

public final class BoundParameterQuery extends Query {

  private BoundParameterQuery(final String command, final String database) {
    super(command, database);
  }

  public static class QueryBuilder {
    private BoundParameterQuery query;
    private String influxQL;

    public static QueryBuilder newQuery(final String influxQL) {
      QueryBuilder instance = new QueryBuilder();
      instance.influxQL = influxQL;
      return instance;
    }

    public QueryBuilder forDatabase(final String database) {
      query = new BoundParameterQuery(influxQL, database);
      return this;
    }

    public QueryBuilder bind(final String placeholder, final Object value) {
      if (query == null) {
          query = new BoundParameterQuery(influxQL, null);
      }
      query.bindParameter(placeholder, value);
      return this;
    }

    public BoundParameterQuery create() {
      return query;
    }
  }
}

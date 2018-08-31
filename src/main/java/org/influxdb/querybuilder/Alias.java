package org.influxdb.querybuilder;

public class Alias {

  private final Object column;
  private final String alias;

  public Alias(final Object column, final String alias) {
    this.column = column;
    this.alias = alias;
  }

  public Object getColumn() {
    return column;
  }

  public String getAlias() {
    return alias;
  }
}

package org.influxdb.querybuilder;

public class Column {

  private final String name;

  Column(final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}

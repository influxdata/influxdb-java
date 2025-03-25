package org.influxdb.querybuilder;

public class Placeholder {

  private final String name;

  Placeholder(final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}

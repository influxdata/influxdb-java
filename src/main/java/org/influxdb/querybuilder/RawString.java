package org.influxdb.querybuilder;

public class RawString {

  private final String str;

  public RawString(String str) {
    this.str = str;
  }

  @Override
  public String toString() {
    return str;
  }
}

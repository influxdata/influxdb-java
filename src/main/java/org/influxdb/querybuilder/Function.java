package org.influxdb.querybuilder;

public class Function {

  private final String name;
  private final Object[] parameters;

  Function(final String name, final Object... parameters) {
    this.name = name;
    this.parameters = parameters;
  }

  public String getName() {
    return name;
  }

  public Object[] getParameters() {
    return parameters;
  }
}

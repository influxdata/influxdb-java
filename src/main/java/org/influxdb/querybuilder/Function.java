package org.influxdb.querybuilder;

public class Function {

  private final String name;
  private final Object[] parameters;

  Function(String name, Object... parameters) {
    this.name = name;
    this.parameters = parameters;
  }

  public String getName() {
    return name;
  }

  public Object[] getParameters() {
    return parameters;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name).append('(');
    for (int i = 0; i < parameters.length; i++) {
      if (i > 0) sb.append(',');
      sb.append(parameters[i]);
    }
    sb.append(')');
    return sb.toString();
  }
}

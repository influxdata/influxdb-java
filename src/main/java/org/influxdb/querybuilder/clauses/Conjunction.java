package org.influxdb.querybuilder.clauses;

public interface Conjunction {

  void join(StringBuilder stringBuilder);
}

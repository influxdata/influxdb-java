package org.influxdb.querybuilder.clauses;

public abstract class AbstractClause implements Clause {

  final String name;

  AbstractClause(String name) {
    this.name = name;
  }
}

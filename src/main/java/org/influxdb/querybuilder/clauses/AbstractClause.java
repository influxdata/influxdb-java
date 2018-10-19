package org.influxdb.querybuilder.clauses;

public abstract class AbstractClause implements Clause {

  final String name;

  AbstractClause(final String name) {
    this.name = name;
  }
}

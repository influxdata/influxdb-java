package org.influxdb.querybuilder;

import org.influxdb.querybuilder.clauses.OperationClause;
import org.influxdb.querybuilder.clauses.SimpleClause;

public interface Selection {

  Selection distinct();

  Selection as(final String aliasName);

  Selection all();

  Selection countAll();

  Selection regex(final String clause);

  Selection column(final String name);

  Selection function(final String name, final Object... parameters);

  Selection raw(final String text);

  Selection count(final Object column);

  Selection max(final Object column);

  Selection min(final Object column);

  Selection sum(final Object column);

  Selection mean(final Object column);

  Selection op(final OperationClause operationClause);

  Selection op(final Object arg1, final String op, final Object arg2);

  Selection cop(final SimpleClause simpleClause);

  Selection cop(final String column, final String op, final Object arg2);
}

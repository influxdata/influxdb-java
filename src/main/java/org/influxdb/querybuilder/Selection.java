package org.influxdb.querybuilder;

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
}

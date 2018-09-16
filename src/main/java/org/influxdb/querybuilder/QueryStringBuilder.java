package org.influxdb.querybuilder;

public interface QueryStringBuilder {

  StringBuilder buildQueryString(final StringBuilder stringBuilder);

  StringBuilder buildQueryString();
}

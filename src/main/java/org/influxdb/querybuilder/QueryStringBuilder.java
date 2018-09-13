package org.influxdb.querybuilder;

interface QueryStringBuilder {

  StringBuilder buildQueryString(final StringBuilder stringBuilder);

  StringBuilder buildQueryString();
}

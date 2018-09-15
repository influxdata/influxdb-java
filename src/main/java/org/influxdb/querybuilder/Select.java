package org.influxdb.querybuilder;

import org.influxdb.querybuilder.clauses.Clause;

public interface Select {

  <T extends Where> T where();

  <T extends Where> T where(final Clause clause);

  <T extends Where> T where(final String text);

  <T extends Select> T orderBy(final Ordering ordering);

  <T extends Select> T groupBy(final Object... columns);

  <T extends Select> T fill(final Number value);

  <T extends Select> T fill(final String value);

  <T extends Select> T limit(final int limit);

  <T extends Select> T limit(final int limit, final long offSet);

  <T extends Select> T sLimit(final int sLimit);

  <T extends Select> T sLimit(final int sLimit, final long sOffSet);

  <T extends Select> T tz(final String timezone);
}

package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.time.TimeInterval;

public class SubRelativeTimeClause extends RelativeTimeClause {

  public SubRelativeTimeClause(final TimeInterval timeInterval) {
    super("-", timeInterval);
  }
}

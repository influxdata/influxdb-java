package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.time.TimeInterval;

public class AddRelativeTimeClause extends RelativeTimeClause {

  public AddRelativeTimeClause(final TimeInterval timeInterval) {
    super("+", timeInterval);
  }
}

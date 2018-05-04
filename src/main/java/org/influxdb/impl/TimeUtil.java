package org.influxdb.impl;

import java.text.SimpleDateFormat;
import java.util.EnumSet;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Utils for time related methods.
 *
 * @author stefan.majer [at] gmail.com
 */
public enum TimeUtil {
  INSTANCE;

    private static final ThreadLocal<SimpleDateFormat> FORMATTER_MILLIS = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat dateDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            dateDF.setTimeZone(TimeZone.getTimeZone("UTC"));
            return dateDF;
        }
    };

    private static final ThreadLocal<SimpleDateFormat> FORMATTER_SECONDS = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat dateDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            dateDF.setTimeZone(TimeZone.getTimeZone("UTC"));
            return dateDF;
        }
    };

  private static final EnumSet<TimeUnit> ALLOWED_TIMEUNITS = EnumSet.of(
      TimeUnit.HOURS,
      TimeUnit.MINUTES,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS,
      TimeUnit.MICROSECONDS,
      TimeUnit.NANOSECONDS);

    public static final int TIME_IN_SECOND_LENGTH = 20;

  /**
   * Convert from a TimeUnit to a influxDB timeunit String.
   *
   * @param t the TimeUnit
   * @return the String representation.
   */
  public static String toTimePrecision(final TimeUnit t) {
    switch (t) {
    case HOURS:
      return "h";
    case MINUTES:
      return "m";
    case SECONDS:
      return "s";
    case MILLISECONDS:
      return "ms";
    case MICROSECONDS:
      return "u";
    case NANOSECONDS:
      return "n";
    default:
      throw new IllegalArgumentException("time precision must be one of:" + ALLOWED_TIMEUNITS);
    }
  }

    /**
     * convert a unix epoch time to timestamp used by influxdb.
     * this can then be used in query expressions against influxdb's time column like so:
     * influxDB.query(new Query("SELECT * FROM some_measurement WHERE time &gt;= '"
     *                          + toInfluxDBTimeFormat(timeStart) + "'", some_database))
     * influxdb time format example: 2016-10-31T06:52:20.020Z
     *
     * @param time timestamp to use, in unix epoch time
     * @return influxdb compatible date-tome string
     */
    public static String toInfluxDBTimeFormat(final long time) {
        return FORMATTER_MILLIS.get().format(time);
    }

    /**
     * convert an influxdb timestamp used by influxdb to unix epoch time.
     * influxdb time format example: 2016-10-31T06:52:20.020Z or 2016-10-31T06:52:20Z
     *
     * @param time timestamp to use, in influxdb datetime format
     * @return time in unix epoch time
     */
    public static long fromInfluxDBTimeFormat(final String time) {
        try {
            if (time.length() == TIME_IN_SECOND_LENGTH) {
                return FORMATTER_SECONDS.get().parse(time).getTime();
            }
            return FORMATTER_MILLIS.get().parse(time).getTime();
        } catch (Exception e) {
            throw new RuntimeException("unexpected date format", e);
        }
    }

}

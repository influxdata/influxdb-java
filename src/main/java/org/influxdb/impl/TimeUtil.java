package org.influxdb.impl;

import java.text.SimpleDateFormat;
import java.util.EnumSet;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Utils for time related methods.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public enum TimeUtil {
  ;

  private static final EnumSet<TimeUnit> ALLOWED_TIMEUNITS = EnumSet.of(
      TimeUnit.HOURS,
      TimeUnit.MINUTES,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS,
      TimeUnit.MICROSECONDS,
      TimeUnit.NANOSECONDS);

  /**
   * Convert from a TimeUnit to a influxDB timeunit String.
   *
   * @param t
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
     * influxDB.query(new Query("SELECT * FROM some_measurement WHERE time >= '"
     *                          + toInfluxDBTimeFormat(timeStart) + "'", some_database))
     * influxdb time format example: 2016-10-31T06:52:20.020Z
     *
     * @param time timestamp to use, in unix epoch time
     * @return influxdb compatible date-tome string
     */
    public static String toInfluxDBTimeFormat(long time) {
        SimpleDateFormat dateDF = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeDF = new SimpleDateFormat("HH:mm:ss.SSS");
        dateDF.setTimeZone(TimeZone.getTimeZone("UTC"));
        timeDF.setTimeZone(TimeZone.getTimeZone("UTC"));

        return dateDF.format(time) + "T" + timeDF.format(time) + "Z";
    }

    /**
     * convert an influxdb timestamp used by influxdb to unix epoch time.
     * influxdb time format example: 2016-10-31T06:52:20.020Z
     *
     * @param time timestamp to use, in influxdb datetime format
     * @return time in unix epoch time
     */
    public static long fromInfluxDBTimeFormat(String time) {
        try {
            String[] parts = time.split("T");
            String datePart = parts[0];
            String timePart = parts[1].substring(0, parts[1].length() - 1);
            SimpleDateFormat dateDF = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat timeDF = new SimpleDateFormat("HH:mm:ss.SSS");
            dateDF.setTimeZone(TimeZone.getTimeZone("UTC"));
            timeDF.setTimeZone(TimeZone.getTimeZone("UTC"));

            return dateDF.parse(datePart).getTime() + timeDF.parse(timePart).getTime();
        } catch (Exception e) {
            throw new RuntimeException("unexpected date format", e);
        }
    }

}

package org.influxdb;

/**
 * A wrapper for various exceptions caused while interacting with InfluxDB.
 *
 * @author Simon Legner
 */
public class InfluxDBException extends RuntimeException {

  public InfluxDBException(final String message) {
    super(message);
  }

  public InfluxDBException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public InfluxDBException(final Throwable cause) {
    super(cause);
  }
}

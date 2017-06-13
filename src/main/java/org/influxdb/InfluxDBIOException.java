package org.influxdb;

import java.io.IOException;

/**
 * A wrapper for {@link IOException} caused while interacting with InfluxDB.
 *
 * @author Simon Legner
 */
public class InfluxDBIOException extends InfluxDBException {

  public InfluxDBIOException(final IOException cause) {
    super(cause);
  }
}

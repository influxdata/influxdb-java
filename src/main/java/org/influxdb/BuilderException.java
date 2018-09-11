package org.influxdb;

/**
 * Class for exceptions when using Point Builder.
 *
 * @author mirza99
 */
public class BuilderException extends RuntimeException {

  /**
   * Generated serial version UID.
   */
  private static final long serialVersionUID = 4178882805281378918L;

  public BuilderException(final String message) {
    super(message);
  }
}

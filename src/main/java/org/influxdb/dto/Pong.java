package org.influxdb.dto;

/**
 * Representation of the response for a influxdb ping.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class Pong {
  private String version;
  private long responseTime;

  /**
   * @return the status
   */
  public String getVersion() {
    return this.version;
  }

  /**
   * @param version
   *            the status to set
   */
  public void setVersion(final String version) {
    this.version = version;
  }

  /**
   * Good or bad connection status.
   */
  public boolean isGood() {
    return !"unknown".equalsIgnoreCase(version);
  }

  /**
   * @return the responseTime
   */
  public long getResponseTime() {
    return this.responseTime;
  }

  /**
   * @param responseTime
   *            the responseTime to set
   */
  public void setResponseTime(final long responseTime) {
    this.responseTime = responseTime;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "Pong{version=" + version + ", responseTime=" + responseTime + "}";
  }

}

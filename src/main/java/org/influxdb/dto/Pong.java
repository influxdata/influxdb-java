package org.influxdb.dto;

public class Pong {
	private String status;
	private long responseTime;

	public String getStatus() {
		return this.status;
	}

	public void setStatus(final String status) {
		this.status = status;
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
}
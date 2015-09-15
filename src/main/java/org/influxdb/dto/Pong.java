package org.influxdb.dto;

import com.google.common.base.MoreObjects;

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
		return version;
	}

	/**
	 * @param version
	 *            the status to set
	 */
	public void setVersion(String version) {
		this.version = version;
	}

	/**
	 * @return the responseTime
	 */
	public long getResponseTime() {
		return responseTime;
	}

	/**
	 * @param responseTime
	 *            the responseTime to set
	 */
	public void setResponseTime(long responseTime) {
		this.responseTime = responseTime;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return MoreObjects
				.toStringHelper(getClass())
				.add("version", version)
				.add("responseTime", responseTime)
				.toString();
	}

}
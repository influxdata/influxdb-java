package org.influxdb.dto;

import java.util.Map;

/**
 * Representation of a InfluxDB database Point.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Point {
	private String name;
	private Long time;
	private Map<String, String> tags;
	private String precision;
	private Map<String, Object> fields;

	/**
	 * @return the measurement
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @param measurement
	 *            the measurement to set
	 */
	public void setName(final String measurement) {
		this.name = measurement;
	}

	/**
	 * @return the time
	 */
	public Long getTime() {
		return this.time;
	}

	/**
	 * @param time
	 *            the time to set
	 */
	public void setTime(final Long time) {
		this.time = time;
	}

	/**
	 * @return the tags
	 */
	public Map<String, String> getTags() {
		return this.tags;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	public void setTags(final Map<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * @return the precision
	 */
	public String getPrecision() {
		return this.precision;
	}

	/**
	 * @param precision
	 *            the precision to set
	 */
	public void setPrecision(final String precision) {
		this.precision = precision;
	}

	/**
	 * @return the fields
	 */
	public Map<String, Object> getFields() {
		return this.fields;
	}

	/**
	 * @param fields
	 *            the fields to set
	 */
	public void setFields(final Map<String, Object> fields) {
		this.fields = fields;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Point [name=");
		builder.append(this.name);
		builder.append(", time=");
		builder.append(this.time);
		builder.append(", tags=");
		builder.append(this.tags);
		builder.append(", precision=");
		builder.append(this.precision);
		builder.append(", fields=");
		builder.append(this.fields);
		builder.append("]");
		return builder.toString();
	}

}
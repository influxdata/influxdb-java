package org.influxdb.dto;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

/**
 * Representation of a InfluxDB database Point.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Point {
	private String measurement;
	private Map<String, String> tags;
	/**
	 * The time stored in nanos. FIXME ensure this
	 */
	private Long time;
	private TimeUnit precision = TimeUnit.NANOSECONDS;
	private Map<String, Object> fields;

	Point() {
	}

	/**
	 * Create a new Point Build build to create a new Point in a fluent manner-
	 *
	 * @param measurement
	 *            the name of the measurement.
	 * @return the Builder to be able to add further Builder calls.
	 */

	public static Builder measurement(final String measurement) {
		return new Builder(measurement);
	}

	/**
	 * Builder for a new Point.
	 *
	 * @author stefan.majer [at] gmail.com
	 *
	 */
	public static class Builder {
		private final String measurement;
		private final Map<String, String> tags = Maps.newTreeMap(Ordering.natural());
		private Long time;
		private TimeUnit precision = TimeUnit.NANOSECONDS;
		private final Map<String, Object> fields = Maps.newTreeMap(Ordering.natural());

		/**
		 * @param measurement
		 */
		Builder(final String measurement) {
			this.measurement = measurement;
		}

		/**
		 * Add a tag to this point.
		 *
		 * @param tagName
		 *            the tag name
		 * @param value
		 *            the tag value
		 * @return the Builder instance.
		 */
		public Builder tag(final String tagName, final String value) {
			this.tags.put(tagName, value);
			return this;
		}

		/**
		 * Add a field to this point.
		 *
		 * @param field
		 *            the field name
		 * @param value
		 *            the value of this field
		 * @return the Builder instance.
		 */
		public Builder field(final String field, final Object value) {
			this.fields.put(field, value);
			return this;
		}

		/**
		 * Add a time to this point
		 *
		 * @param precisionToSet
		 * @param timeToSet
		 * @return the Builder instance.
		 */
		public Builder time(final long timeToSet, final TimeUnit precisionToSet) {
			// FIXME convert to millis.
			this.precision = precisionToSet;
			this.time = timeToSet;
			return this;
		}

		/**
		 * Create a new Point.
		 *
		 * @return the newly created Point.
		 */
		public Point build() {
			Preconditions.checkArgument(
					!Strings.isNullOrEmpty(this.measurement),
					"Point name must not be null or empty.");
			Point point = new Point();
			point.setFields(this.fields);
			point.setMeasurement(this.measurement);
			point.setPrecision(this.precision);
			point.setTags(this.tags);
			point.setTime(this.time);
			return point;
		}
	}

	/**
	 * @param measurement
	 *            the measurement to set
	 */
	void setMeasurement(final String measurement) {
		this.measurement = measurement;
	}

	/**
	 * @param time
	 *            the time to set
	 */
	void setTime(final Long time) {
		this.time = time;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	void setTags(final Map<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * @return the tags
	 */
	Map<String, String> getTags() {
		return this.tags;
	}

	/**
	 * @param precision
	 *            the precision to set
	 */
	void setPrecision(final TimeUnit precision) {
		this.precision = precision;
	}

	/**
	 * @param fields
	 *            the fields to set
	 */
	void setFields(final Map<String, Object> fields) {
		this.fields = fields;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Point [name=");
		builder.append(this.measurement);
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

	// measurement[,tag=value,tag2=value2...] field=value[,field2=value2...] [unixnano]

	/**
	 * calculate the lineprotocol entry for a single Point.
	 * 
	 * Documentation is WIP : https://github.com/influxdb/influxdb/pull/2997
	 *
	 * @return the String without newLine.
	 */
	public String lineProtocol() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.measurement);
		for (Entry<String, String> tag : this.tags.entrySet()) {
			sb.append(",");
			String value = tag.getValue().replace(" ", "\\ ");
			sb.append(tag.getKey()).append("=").append(value);
		}
		sb.append(" ");
		int fieldCount = this.fields.size();
		int loops = 0;
		for (Entry<String, Object> field : this.fields.entrySet()) {
			loops++;
			sb.append(field.getKey()).append("=").append(field.getValue());
			if (loops < fieldCount) {
				sb.append(",");
			}
		}
		if (null == this.time) {
			this.time = System.currentTimeMillis();
		}
		if (this.time != null) {
			sb.append(" ").append(TimeUnit.NANOSECONDS.convert(this.time, this.precision));
		}
		return sb.toString();
	}
}
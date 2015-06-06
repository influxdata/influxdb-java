package org.influxdb.dto;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.influxdb.impl.InfluxDBImpl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

/**
 * Representation of a InfluxDB database Point.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Point {
	private String name;
	private Map<String, String> tags;
	private Long time;
	private String precision;
	private Map<String, Object> fields;

	Point() {
	}

	/**
	 * Builder for a new Point.
	 *
	 * @author stefan.majer [at] gmail.com
	 *
	 */
	public static class Builder {
		private final String name;
		private final Map<String, String> tags = Maps.newHashMap();
		private Long time;
		private String precision;
		private final Map<String, Object> fields = Maps.newHashMap();

		/**
		 * @param name
		 */
		public Builder(final String name) {
			this.name = name;
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
			this.precision = InfluxDBImpl.toTimePrecision(precisionToSet);
			this.time = timeToSet;
			return this;
		}

		/**
		 * Create a new Point.
		 *
		 * @return the newly created Point.
		 */
		public Point build() {
			Preconditions.checkArgument(!Strings.isNullOrEmpty(this.name), "Point name must not be null or empty.");
			Point point = new Point();
			point.setFields(this.fields);
			point.setName(this.name);
			point.setPrecision(this.precision);
			point.setTags(this.tags);
			point.setTime(this.time);
			return point;
		}
	}

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
	void setName(final String measurement) {
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
	void setTime(final Long time) {
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
	void setTags(final Map<String, String> tags) {
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
	void setPrecision(final String precision) {
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
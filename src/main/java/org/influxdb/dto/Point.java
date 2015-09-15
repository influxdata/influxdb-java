package org.influxdb.dto;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Representation of a InfluxDB database Point.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Point {
	private String measurement;
	private Map<String, String> tags;
	private Long time;
	private TimeUnit precision = TimeUnit.NANOSECONDS;
	private Map<String, Object> fields;

	private static final Escaper FIELD_ESCAPER = Escapers.builder().addEscape('"', "\\\"").build();
	private static final Escaper KEY_ESCAPER = Escapers.builder().addEscape(' ', "\\ ").addEscape(',', "\\,").build();

	Point() {
	}

	/**
	 * Create a new Point Build build to create a new Point in a fluent manner-
	 *
	 * @param measurement
	 *            the name of the measurement.
	 * @return the Builder to be able to add further Builder calls.
	 */

	public static Builder measurement(String measurement) {
		return new Builder(measurement);
	}

	/**
	 * Builder for a new Point.
	 *
	 * @author stefan.majer [at] gmail.com
	 *
	 */
	public static final class Builder {
		private final String measurement;
		private final Map<String, String> tags = Maps.newTreeMap(Ordering.natural());
		private Long time;
		private TimeUnit precision = TimeUnit.NANOSECONDS;
		private final Map<String, Object> fields = Maps.newTreeMap(Ordering.natural());

		/**
		 * @param measurement
		 */
		Builder(String measurement) {
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
		public Builder tag(String tagName, String value) {
         tags.put(tagName, value);
			return this;
		}

		/**
		 * Add a Map of tags to add to this point.
		 *
		 * @param tagsToAdd
		 *            the Map of tags to add
		 * @return the Builder instance.
		 */
		public Builder tag(Map<String, String> tagsToAdd) {
         tags.putAll(tagsToAdd);
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
		public Builder field(String field, Object value) {
         fields.put(field, value);
			return this;
		}

		/**
		 * Add a Map of fields to this point.
		 *
		 * @param fieldsToAdd
		 *            the fields to add
		 * @return the Builder instance.
		 */
		public Builder fields(Map<String, Object> fieldsToAdd) {
         fields.putAll(fieldsToAdd);
			return this;
		}

		/**
		 * Add a time to this point
		 *
		 * @param precisionToSet
		 * @param timeToSet
		 * @return the Builder instance.
		 */
		public Builder time(long timeToSet, TimeUnit precisionToSet) {
		    Preconditions.checkNotNull(precisionToSet, "Precision must be not null!");
         time = timeToSet;
         precision = precisionToSet;
			return this;
		}

		/**
		 * Create a new Point.
		 *
		 * @return the newly created Point.
		 */
		public Point build() {
			Preconditions
					.checkArgument(!Strings.isNullOrEmpty(measurement), "Point name must not be null or empty.");
			Preconditions.checkArgument(fields.size() > 0, "Point must have at least one field specified.");
			Point point = new Point();
			point.setFields(fields);
			point.setMeasurement(measurement);
			if (time != null) {
			    point.setTime(time);
			    point.setPrecision(precision);
			} else {
			    point.setTime(System.currentTimeMillis());
			    point.setPrecision(TimeUnit.MILLISECONDS);
			}
			point.setTags(tags);
			return point;
		}
	}

	/**
	 * @param measurement
	 *            the measurement to set
	 */
	void setMeasurement(String measurement) {
		this.measurement = measurement;
	}

	/**
	 * @param time
	 *            the time to set
	 */
	void setTime(Long time) {
		this.time = time;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * @return the tags
	 */
	Map<String, String> getTags() {
		return tags;
	}

	/**
	 * @param precision
	 *            the precision to set
	 */
	void setPrecision(TimeUnit precision) {
		this.precision = precision;
	}

	/**
	 * @param fields
	 *            the fields to set
	 */
	void setFields(Map<String, Object> fields) {
		this.fields = fields;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Point [name=");
		builder.append(measurement);
		builder.append(", time=");
		builder.append(time);
		builder.append(", tags=");
		builder.append(tags);
		builder.append(", precision=");
		builder.append(precision);
		builder.append(", fields=");
		builder.append(fields);
		builder.append("]");
		return builder.toString();
	}

	/**
	 * calculate the lineprotocol entry for a single Point.
	 * 
	 * Documentation is WIP : https://github.com/influxdb/influxdb/pull/2997
	 * 
	 * https://github.com/influxdb/influxdb/blob/master/tsdb/README.md
	 *
	 * @return the String without newLine.
	 */
	public String lineProtocol() {
		StringBuilder sb = new StringBuilder();
		sb.append(KEY_ESCAPER.escape(measurement));
		sb.append(concatenatedTags());
		sb.append(concatenateFields());
		sb.append(formatedTime());
		return sb.toString();
	}

	private StringBuilder concatenatedTags() {
		StringBuilder sb = new StringBuilder();
		for (Entry<String, String> tag : tags.entrySet()) {
			sb.append(",");
			sb.append(KEY_ESCAPER.escape(tag.getKey())).append("=").append(KEY_ESCAPER.escape(tag.getValue()));
		}
		sb.append(" ");
		return sb;
	}

	private StringBuilder concatenateFields() {
		StringBuilder sb = new StringBuilder();
		int fieldCount = fields.size();
		int loops = 0;

		NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
		numberFormat.setMaximumFractionDigits(340);
		numberFormat.setGroupingUsed(false);
		numberFormat.setMinimumFractionDigits(1);

		for (Entry<String, Object> field : fields.entrySet()) {
			sb.append(KEY_ESCAPER.escape(field.getKey())).append("=");
			loops++;
			Object value = field.getValue();
			if (value instanceof String) {
				String stringValue = (String) value;
				sb.append("\"").append(FIELD_ESCAPER.escape(stringValue)).append("\"");
			} else if (value instanceof Number) {
				sb.append(numberFormat.format(value));
			} else {
				sb.append(value);
			}

			if (loops < fieldCount) {
				sb.append(",");
			}
		}
		return sb;
	}

	private StringBuilder formatedTime() {
		StringBuilder sb = new StringBuilder();
		if (null == time) {
         time = System.nanoTime();
		}
		sb.append(" ").append(TimeUnit.NANOSECONDS.convert(time, precision));
		return sb;
	}

}

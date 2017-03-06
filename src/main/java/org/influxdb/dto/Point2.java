package org.influxdb.dto;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

/**
 * Representation of a InfluxDB database Point.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Point2 {
	private String lineProtocol = null;

	private static final Escaper FIELD_ESCAPER = Escapers.builder().addEscape('"', "\\\"").build();
	private static final Escaper KEY_ESCAPER = Escapers.builder().addEscape(' ', "\\ ").addEscape(',', "\\,").addEscape('=', "\\=").build();

	public Point2(String lineProtocol) {
		this.lineProtocol = lineProtocol;
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
	public static final class Builder {
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
			Preconditions.checkArgument(tagName != null);
			Preconditions.checkArgument(value != null);
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
		public Builder tag(final Map<String, String> tagsToAdd) {
			for (Entry<String, String> tag : tagsToAdd.entrySet()) {
				tag(tag.getKey(), tag.getValue());
			}
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
		@Deprecated
		public Builder field(final String field, Object value) {
			if (value instanceof Number) {
				if (value instanceof Byte) {
					value = ((Byte) value).doubleValue();
				}
				if (value instanceof Short) {
					value = ((Short) value).doubleValue();
				}
				if (value instanceof Integer) {
					value = ((Integer) value).doubleValue();
				}
				if (value instanceof Long) {
					value = ((Long) value).doubleValue();
				}
				if (value instanceof BigInteger) {
					value = ((BigInteger) value).doubleValue();
				}
				
			}
			fields.put(field, value);
			return this;
		}
		
		public Builder addField(final String field, final boolean value) {
			fields.put(field, value);
			return this;
		}
		
		public Builder addField(final String field, final long value) {
			fields.put(field, value);
			return this;
		}
		
		public Builder addField(final String field, final double value) {
			fields.put(field, value);
			return this;
		}
		
		public Builder addField(String field, Number value) {
			fields.put(field, value);
			return this;
		}
		
		public Builder addField(final String field, final String value) {
			if (value == null) {
				throw new IllegalArgumentException("Field value cannot be null");
			}
			
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
		public Builder fields(final Map<String, Object> fieldsToAdd) {
			this.fields.putAll(fieldsToAdd);
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
		    Preconditions.checkNotNull(precisionToSet, "Precision must be not null!");
			this.time = timeToSet;
			this.precision = precisionToSet;
			return this;
		}

		/**
		 * Create a new Point.
		 *
		 * @return the newly created Point.
		 */
		public Point2 build() {
			Preconditions
					.checkArgument(!Strings.isNullOrEmpty(this.measurement), "Point name must not be null or empty.");
			Preconditions.checkArgument(this.fields.size() > 0, "Point must have at least one field specified.");
			if (time == null) {
			    time = System.currentTimeMillis();
			    precision = TimeUnit.MILLISECONDS;
			}
			
			return new Point2(lineProtocol(measurement, tags, fields, time, precision));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Point [");
		builder.append(this.lineProtocol);
		builder.append("]");
		return builder.toString();
	}

	/**
	 * calculate the lineprotocol entry for a single Point.
	 * 
	 * Documentation is WIP : https://github.com/influxdb/influxdb/pull/2997
	 * 
	 * https://github.com/influxdb/influxdb/blob/master/tsdb/README.md
	 * @param precision 
	 *
	 * @return the String without newLine.
	 */
	public static String lineProtocol(String measurement, Map<String, String> tags, Map<String, Object> fields, Long time, TimeUnit precision) {
		
		final StringBuilder sb = new StringBuilder();
		sb.append(KEY_ESCAPER.escape(measurement));
		sb.append(concatenatedTags(tags));
		sb.append(concatenateFields(fields));
		sb.append(formatedTime(time, precision));
		return sb.toString();
	}

	private static StringBuilder concatenatedTags(Map<String, String> tags) {
		final StringBuilder sb = new StringBuilder();
		for (Entry<String, String> tag : tags.entrySet()) {
			sb.append(",");
			sb.append(KEY_ESCAPER.escape(tag.getKey())).append("=").append(KEY_ESCAPER.escape(tag.getValue()));
		}
		sb.append(" ");
		return sb;
	}
	
	private static StringBuilder concatenateFields(Map<String, Object> fields) {
		final StringBuilder sb = new StringBuilder();
		final int fieldCount = fields.size();
		int loops = 0;

		NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
		numberFormat.setMaximumFractionDigits(340);
		numberFormat.setGroupingUsed(false);
		numberFormat.setMinimumFractionDigits(1);

		for (Entry<String, Object> field : fields.entrySet()) {
			loops++;
			Object value = field.getValue();
			if (value == null) {
				continue;
			}

			sb.append(KEY_ESCAPER.escape(field.getKey())).append("=");
			if (value instanceof String) {
				String stringValue = (String) value;
				sb.append("\"").append(FIELD_ESCAPER.escape(stringValue)).append("\"");
			} else if (value instanceof Number) {
				if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
					sb.append(numberFormat.format(value));
				} else {
					sb.append(value).append("i");
				}
			} else {
				sb.append(value);
			}

			if (loops < fieldCount) {
				sb.append(",");
			}
		}

		return sb;
	}

	private static StringBuilder formatedTime(Long time, TimeUnit precision) {
		final StringBuilder sb = new StringBuilder();
		if (null == time) {
			time = System.nanoTime();
		}
		sb.append(" ").append(TimeUnit.NANOSECONDS.convert(time, precision));
		return sb;
	}

}

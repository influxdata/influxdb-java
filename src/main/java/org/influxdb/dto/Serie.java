package org.influxdb.dto;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Representation of a InfluxDB database serie.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Serie {
	private final String name;
	String[] columns;
	Object[][] points;

	/**
	 * Builder for a new Serie.
	 * 
	 * A typical usage would look like:
	 * 
	 * <br/>
	 * <code>
	 * Serie serie = new Serie.Builder("serieName")
	 * 			.columns("time", "cpu_idle")
	 * 			.values(System.currentTimeMillis(), 0.97)
	 * 			.values(System.currentTimeMillis(), 0.99).build();
	 * </code>
	 */
	public static class Builder {
		private final String name;
		private final List<String> columns = Lists.newArrayList();
		private final List<List<Object>> valueRows = Lists.newArrayList();

		/**
		 * @param name
		 *            the name of the Serie to create.
		 */
		public Builder(final String name) {
			super();
			this.name = name;
		}

		/**
		 * @param columnNames
		 *            the array of names of all columns.
		 * @return this Builder instance.
		 */
		public Builder columns(final String... columnNames) {
			Preconditions.checkArgument(this.columns.isEmpty(), "You can only call columns() once.");
			this.columns.addAll(Arrays.asList(columnNames));
			return this;
		}

		/**
		 * @param values
		 *            the values for a single row.
		 * @return this Builder instance.
		 */
		public Builder values(final Object... values) {
			Preconditions.checkArgument(values.length == this.columns.size(), "Value count differs from column count.");
			this.valueRows.add(Arrays.asList(values));
			return this;
		}

		/**
		 * Create a Serie instance.
		 * 
		 * @return the created Serie.
		 */
		public Serie build() {
			Preconditions.checkArgument(!Strings.isNullOrEmpty(this.name), "Serie name must not be null or empty.");
			Serie serie = new Serie(this.name);
			serie.columns = this.columns.toArray(new String[this.columns.size()]);
			Object[][] points = new Object[this.valueRows.size()][this.columns.size()];
			int row = 0;
			for (List<Object> values : this.valueRows) {
				points[row] = values.toArray();
				row++;
			}
			serie.points = points;
			return serie;
		}
	}

	/**
	 * @param name
	 *            the name of the serie.
	 */
	Serie(final String name) {
		this.name = name;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @return the columns
	 */
	public String[] getColumns() {
		return this.columns;
	}

	/**
	 * @return the Points as a List of Maps with columnName to value.
	 */
	public List<Map<String, Object>> getRows() {
		List<Map<String, Object>> rows = Lists.newArrayList();
		for (Object[] point : this.points) {
			int column = 0;
			Map<String, Object> row = Maps.newHashMap();
			for (Object value : point) {
				row.put(this.columns[column], value);
				column++;
			}
			rows.add(row);
		}
		return rows;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return Objects
				.toStringHelper(this.getClass())
				.add("name", this.name)
				.add("c", Arrays.deepToString(this.columns))
				.add("p", Arrays.deepToString(this.points))
				.toString();
	}

}
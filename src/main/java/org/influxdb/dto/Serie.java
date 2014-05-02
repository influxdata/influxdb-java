package org.influxdb.dto;

import java.util.Arrays;

import com.google.common.base.Objects;

/**
 * Representation of a InfluxDB database serie.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Serie {
	private final String name;
	private String[] columns;
	private Object[][] points;

	/**
	 * @param name
	 *            the name of the serie.
	 */
	public Serie(final String name) {
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
	 * @param columns
	 *            the columns to set
	 */
	public void setColumns(final String[] columns) {
		this.columns = columns;
	}

	/**
	 * @return the points
	 */
	public Object[][] getPoints() {
		return this.points;
	}

	/**
	 * @param points
	 *            the points to set
	 */
	public void setPoints(final Object[][] points) {
		this.points = points;
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
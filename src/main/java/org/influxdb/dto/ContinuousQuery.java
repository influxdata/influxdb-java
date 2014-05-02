package org.influxdb.dto;

import com.google.common.base.Objects;

/**
 * Representation of a InfluxDB continous_query.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class ContinuousQuery {
	private int id;
	private String query;

	/**
	 * @return the id
	 */
	public int getId() {
		return this.id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(final int id) {
		this.id = id;
	}

	/**
	 * @return the query
	 */
	public String getQuery() {
		return this.query;
	}

	/**
	 * @param query
	 *            the query to set
	 */
	public void setQuery(final String query) {
		this.query = query;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("id", this.id).add("query", this.query).toString();
	}

}
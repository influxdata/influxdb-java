package org.influxdb.dto;

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

}
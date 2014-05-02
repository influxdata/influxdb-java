package org.influxdb.dto;

import com.google.common.base.Objects;

/**
 * Representation of a influxdb database.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Database {
	private final String name;
	private final int replicationFactor;

	/**
	 * @param name
	 *            the name of the database.
	 * @param replicationFactor
	 *            the replicationfactor for the metrics data stored in this database. Must be >= 1.
	 */
	public Database(final String name, final int replicationFactor) {
		super();
		this.name = name;
		this.replicationFactor = replicationFactor;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @return the replicationFactor
	 */
	public int getReplicationFactor() {
		return this.replicationFactor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return Objects
				.toStringHelper(this.getClass())
				.add("name", this.name)
				.add("replicationFactor", this.replicationFactor)
				.toString();
	}

}
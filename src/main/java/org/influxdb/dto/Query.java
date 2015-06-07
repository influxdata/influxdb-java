package org.influxdb.dto;

/**
 * Represents a Query against Influxdb.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class Query {

	private final String command;
	private final String database;

	/**
	 * @param command
	 * @param database
	 */
	public Query(final String command, final String database) {
		super();
		this.command = command;
		this.database = database;
	}

	/**
	 * @return the command
	 */
	public String getCommand() {
		return this.command;
	}

	/**
	 * @return the database
	 */
	public String getDatabase() {
		return this.database;
	}

}

package org.influxdb.dto;

import java.util.Objects;

/**
 * Represents a Query against Influxdb.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class Query {

	private final String command;
	private final String database;

	public Query(String command, String database) {
      this.command = command;
		this.database = database;
	}

	/**
	 * @return the command
	 */
	public String getCommand() {
		return command;
	}

	/**
	 * @return the database
	 */
	public String getDatabase() {
		return database;
	}

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (!(o instanceof Query))
         return false;
      Query query = (Query) o;
      return Objects.equals(getCommand(), query.getCommand()) &&
              Objects.equals(getDatabase(), query.getDatabase());
   }

   @Override
   public int hashCode() {
      return Objects.hash(getCommand(), getDatabase());
   }
}

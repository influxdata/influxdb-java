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
  private final boolean requiresPost;

  /**
   * @param command
   * @param database
   */
  public Query(final String command, final String database) {
    this(command, database, false);
  }

   /**
   * @param command
   * @param database
   * @param requiresPost true if the command requires a POST instead of GET to influxdb
   */
   public Query(final String command, final String database, boolean requiresPost) {
    super();
    this.command = command;
    this.database = database;
    this.requiresPost = requiresPost;
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

  public boolean requiresPost() {
    return requiresPost;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((command == null) ? 0 : command.hashCode());
    result = prime * result
        + ((database == null) ? 0 : database.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Query other = (Query) obj;
    if (command == null) {
      if (other.command != null)
        return false;
    } else if (!command.equals(other.command))
      return false;
    if (database == null) {
      if (other.database != null)
        return false;
    } else if (!database.equals(other.database))
      return false;
    return true;
  }
}

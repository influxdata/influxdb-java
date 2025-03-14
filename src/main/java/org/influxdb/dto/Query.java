package org.influxdb.dto;

import com.squareup.moshi.JsonWriter;
import okio.Buffer;
import org.influxdb.InfluxDBIOException;
import org.influxdb.querybuilder.Appendable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
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
  private final boolean requiresPost;
  protected final Map<String, Object> params = new HashMap<>();

  /**
   * @param command the query command
   */
  public Query(final String command) {
    this(command, null);
  }

  /**
   * @param command the query command
   * @param database the database to query
   */
  public Query(final String command, final String database) {
    this(command, database, false);
  }

   /**
   * @param command the query command
   * @param database the database to query
   * @param requiresPost true if the command requires a POST instead of GET to influxdb
   */
   public Query(final String command, final String database, final boolean requiresPost) {
    super();
    this.command = command;
    this.database = database;
    this.requiresPost = requiresPost;
  }

  /**
   * @return the command
   */
  public String getCommand() {
    return command;
  }

  /**
   * @return url encoded command
   */
  public String getCommandWithUrlEncoded() {
    return encode(command);
  }

  /**
   * @return the database
   */
  public String getDatabase() {
    return database;
  }

  public boolean requiresPost() {
    return requiresPost;
  }

  public Query bindParameter(final String placeholder, final Object value) {
    params.put(placeholder, value);
    return this;
  }

  public boolean hasBoundParameters() {
    return !params.isEmpty();
  }

  public String getParameterJsonWithUrlEncoded() {
    try {
      String jsonParameterObject = createJsonObject(params);
      String urlEncodedJsonParameterObject = encode(jsonParameterObject);
      return urlEncodedJsonParameterObject;
    } catch (IOException e) {
      throw new InfluxDBIOException(e);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Query query = (Query) o;
    return Objects.equals(command, query.command) && Objects.equals(database, query.database) && params.equals(
            query.params);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = Objects.hashCode(command);
    result = prime * result + Objects.hashCode(database);
    result = prime * result + params.hashCode();
    return result;
  }

  /**
   * Encode a command into {@code x-www-form-urlencoded} format.
   * @param command
   *            the command to be encoded.
   * @return a encoded command.
   */
  public static String encode(final String command) {
    try {
      return URLEncoder.encode(command, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Every JRE must support UTF-8", e);
    }
  }

  private String createJsonObject(final Map<String, Object> parameterMap) throws IOException {
    Buffer b = new Buffer();
    JsonWriter writer = JsonWriter.of(b);
    writer.beginObject();
    for (Map.Entry<String, Object> pair : parameterMap.entrySet()) {
      String name = pair.getKey();
      Object value = pair.getValue();
      if (value instanceof Number) {
        Number number = (Number) value;
        writer.name(name).value(number);
      } else if (value instanceof String) {
        writer.name(name).value((String) value);
      } else if (value instanceof Boolean) {
        writer.name(name).value((Boolean) value);
      } else if (value instanceof Appendable) {
        StringBuilder stringBuilder = new StringBuilder();
        ((Appendable) value).appendTo(stringBuilder);
        writer.name(name).value(stringBuilder.toString());
      } else {
        writer.name(name).value(String.valueOf(value));
      }
    }
    writer.endObject();
    return b.readString(Charset.forName("utf-8"));
  }
}

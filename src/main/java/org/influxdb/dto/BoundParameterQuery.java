package org.influxdb.dto;

import com.squareup.moshi.JsonWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.influxdb.InfluxDBIOException;

import okio.Buffer;

public final class BoundParameterQuery extends Query {

  private final Map<String, Object> params = new HashMap<>();

  private BoundParameterQuery(final String command, final String database) {
    super(command, database, true);
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

  private String createJsonObject(final Map<String, Object> parameterMap) throws IOException {
    Buffer b = new Buffer();
    JsonWriter writer = JsonWriter.of(b);
    writer.beginObject();
    for (Entry<String, Object> pair : parameterMap.entrySet()) {
      String name = pair.getKey();
      Object value = pair.getValue();
      if (value instanceof Number) {
        Number number = (Number) value;
        writer.name(name).value(number);
      } else if (value instanceof String) {
        writer.name(name).value((String) value);
      } else if (value instanceof Boolean) {
        writer.name(name).value((Boolean) value);
      } else {
        writer.name(name).value(String.valueOf(value));
      }
    }
    writer.endObject();
    return b.readString(Charset.forName("utf-8"));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + params.hashCode();
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    BoundParameterQuery other = (BoundParameterQuery) obj;
    if (!params.equals(other.params)) {
      return false;
    }
    return true;
  }

  public static class QueryBuilder {
    private BoundParameterQuery query;
    private String influxQL;

    public static QueryBuilder newQuery(final String influxQL) {
      QueryBuilder instance = new QueryBuilder();
      instance.influxQL = influxQL;
      return instance;
    }

    public QueryBuilder forDatabase(final String database) {
      query = new BoundParameterQuery(influxQL, database);
      return this;
    }

    public QueryBuilder bind(final String placeholder, final Object value) {
      query.params.put(placeholder, value);
      return this;
    }

    public BoundParameterQuery create() {
      return query;
    }
  }
}

package org.influxdb.dto;

import com.squareup.moshi.JsonWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import okio.Buffer;

public class BoundParameterQuery extends Query {

    private final Map<String, Object> params = new HashMap<>();

    private BoundParameterQuery(final String command, final String database) {
        super(command, database, true);
    }

    public String getParameterJsonWithUrlEncoded() {
        try {
            List<String> placeholders = parsePlaceHolders(getCommand());
            assurePlaceholdersAreBound(placeholders, params);
            String jsonParameterObject = createJsonObject(params);
            String urlEncodedJsonParameterObject = encode(jsonParameterObject);
            return urlEncodedJsonParameterObject;
        } catch (IOException e) {
            throw new RuntimeException("Couldn't create parameter JSON object", e);
        }
    }

    private void assurePlaceholdersAreBound(List<String> placeholders, Map<String, Object> params) {
        if (placeholders.size() != params.size()) {
            throw new RuntimeException("Unbalanced amount of placeholders and parameters");
        }

        for (String placeholder : placeholders) {
            if (params.get(placeholder) == null) {
                throw new RuntimeException("Placeholder $" + placeholder + " is not bound");
            }
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
                writer.name(name).value((Number) value);
            } else if (value instanceof String) {
                writer.name(name).value((String) value);
            } else if (value instanceof Boolean) {
                writer.name(name).value((Boolean) value);
            } else {
                writer.name(name).value(value.toString());
            }
        }
        writer.endObject();
        return b.readString(Charset.forName("utf-8"));
    }

    private List<String> parsePlaceHolders(final String command) {
        List<String> placeHolders = new ArrayList<>();
        Pattern p = Pattern.compile("\\s+\\$(\\w+?)(?:\\s|$)");
        Matcher m = p.matcher(getCommand());
        while (m.find()) {
            placeHolders.add(m.group(1));
        }
        return placeHolders;
    }

    public static class QueryBuilder {
        private BoundParameterQuery query;
        private String influxQL;

        public static QueryBuilder newQuery(String influxQL) {
            QueryBuilder instance = new QueryBuilder();
            instance.influxQL = influxQL;
            return instance;
        }

        public QueryBuilder forDatabase(String database) {
            query = new BoundParameterQuery(influxQL, database);
            return this;
        }

        public QueryBuilder bind(String placeholder, Object value) {
            query.params.put(placeholder, value);
            return this;
        }

        public BoundParameterQuery create() {
            return query;
        }
    }
}

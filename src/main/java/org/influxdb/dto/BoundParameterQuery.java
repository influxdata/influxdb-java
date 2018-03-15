package org.influxdb.dto;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.squareup.moshi.JsonWriter;

import okio.Buffer;

public class BoundParameterQuery extends Query {

    private final Object[] params;

    public BoundParameterQuery(final String command, final String database, final Object...params) {
        super(command, database, true);
        this.params = params;
    }

    public String getParameterJsonWithUrlEncoded() {
        try {
            List<String> placeholders = parsePlaceHolders(getCommand());
            Map<String, Object> parameterMap = createParameterMap(placeholders, params);
            String jsonParameterObject = createJsonObject(parameterMap);
            String urlEncodedJsonParameterObject = encode(jsonParameterObject);
            return urlEncodedJsonParameterObject;
        } catch (IOException e) {
            throw new RuntimeException("Couldn't create parameter JSON object", e);
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

    private Map<String, Object> createParameterMap(final List<String> placeholders, final Object[] params) {
        if (placeholders.size() != params.length) {
            throw new RuntimeException("Unbalanced amount of placeholders and parameters");
        }

        Map<String, Object> parameterMap = new HashMap<>();
        int index = 0;
        for (String placeholder : placeholders) {
            parameterMap.put(placeholder, params[index++]);
        }
        return parameterMap;
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
}

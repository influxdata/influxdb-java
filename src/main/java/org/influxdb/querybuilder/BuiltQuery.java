package org.influxdb.querybuilder;

import org.influxdb.dto.Query;

public abstract class BuiltQuery extends Query {

    public BuiltQuery(String database) {
        super(null, database);
    }

    public BuiltQuery(String database, boolean requiresPost) {
        super(null, database, requiresPost);
    }

    abstract StringBuilder buildQueryString();

    static StringBuilder addSemicolonIfNeeded(StringBuilder stringBuilder) {
        int length = moveToEndOfText(stringBuilder);
        if (length == 0 || stringBuilder.charAt(length - 1) != ';')
            stringBuilder.append(';');
        return stringBuilder;
    }

    private static int moveToEndOfText(StringBuilder stringBuilder) {
        int length = stringBuilder.length();
        while (length > 0 && stringBuilder.charAt(length - 1) <= ' ')
            length -= 1;
        if (length != stringBuilder.length())
            stringBuilder.setLength(length);
        return length;
    }

    @Override
    public String getCommand() {
        StringBuilder sb = buildQueryString();
        addSemicolonIfNeeded(sb);
        return sb.toString();
    }

    @Override
    public String getCommandWithUrlEncoded() {
        return encode(getCommand());
    }

    @Override
    public String toString() {
        return getCommandWithUrlEncoded();
    }
}

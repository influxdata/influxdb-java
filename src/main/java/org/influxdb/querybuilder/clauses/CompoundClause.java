package org.influxdb.querybuilder.clauses;

import java.util.List;

import org.influxdb.querybuilder.Appender;

public class CompoundClause implements Clause {

    private String operation;
    private final List<String> names;
    private final List<?> values;

    public CompoundClause(List<String> names, String operation, List<?> values) {
        this.operation = operation;
        this.names = names;
        this.values = values;
        if (this.names.size() != this.values.size())
            throw new IllegalArgumentException(String.format("Number of names: (%d) and values: (%d) should match", this.names.size(), this.values.size()));
    }

    @Override
    public void appendTo(StringBuilder stringBuilder) {
        stringBuilder.append("(");
        for (int i = 0; i < names.size(); i++) {
            if (i > 0)
                stringBuilder.append(",");
            Appender.appendName(names.get(i), stringBuilder);
        }
        stringBuilder.append(")").append(operation).append("(");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                stringBuilder.append(",");
            Appender.appendValue(values.get(i), stringBuilder);
        }
        stringBuilder.append(")");
    }

}

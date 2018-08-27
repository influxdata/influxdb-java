package org.influxdb.querybuilder.clauses;

import java.util.List;

import org.influxdb.querybuilder.Appender;

public class CompoundClause implements Clause {

    private String op;
    private final List<String> names;
    private final List<?> values;

    public CompoundClause(List<String> names, String op, List<?> values) {
        this.op = op;
        this.names = names;
        this.values = values;
        if (this.names.size() != this.values.size())
            throw new IllegalArgumentException(String.format("The number of names (%d) and values (%d) don't match", this.names.size(), this.values.size()));
    }

    @Override
    public void appendTo(StringBuilder sb) {
        sb.append("(");
        for (int i = 0; i < names.size(); i++) {
            if (i > 0)
                sb.append(",");
            Appender.appendName(names.get(i), sb);
        }
        sb.append(")").append(op).append("(");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(",");
            Appender.appendValue(values.get(i), sb);
        }
        sb.append(")");
    }

}

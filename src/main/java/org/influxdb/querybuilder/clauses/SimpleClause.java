package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.Appender;

public class SimpleClause extends AbstractClause {

    private final String op;
    private final Object value;

    public SimpleClause(String name, String op, Object value) {
        super(name);
        this.op = op;
        this.value = value;
    }

    @Override
    public void appendTo(StringBuilder sb) {
        Appender.appendName(name, sb).append(op);
        Appender.appendValue(value, sb);
    }

}

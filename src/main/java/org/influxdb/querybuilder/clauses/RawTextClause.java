package org.influxdb.querybuilder.clauses;

import org.influxdb.querybuilder.Appender;
import org.influxdb.querybuilder.RawString;

public class RawTextClause extends AbstractClause {

    private final RawString value;

    public RawTextClause(String text) {
        super("");
        this.value = new RawString(text);

        if(text == null) {
            throw new IllegalArgumentException("Missing text for expression");
        }
    }

    @Override
    public void appendTo(StringBuilder sb) {
        Appender.appendValue(value, sb);
    }
}

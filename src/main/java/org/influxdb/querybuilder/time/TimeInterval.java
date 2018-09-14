package org.influxdb.querybuilder.time;

import org.influxdb.querybuilder.Appendable;

public class TimeInterval implements Appendable {

    private final Long measure;
    private final String literal;

    public TimeInterval(final Long measure, final String literal) {
        this.measure = measure;
        this.literal = literal;
    }

    @Override
    public void appendTo(StringBuilder stringBuilder) {
        stringBuilder.append(measure).append(literal);
    }
}

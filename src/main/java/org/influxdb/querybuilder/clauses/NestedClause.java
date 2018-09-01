package org.influxdb.querybuilder.clauses;

import java.util.List;

import static org.influxdb.querybuilder.Appender.joinAndAppend;

public class NestedClause implements Clause {

    private final List<ConjunctionClause> conjunctionClauses;

    public NestedClause(final List<ConjunctionClause> conjunctionClauses) {
        this.conjunctionClauses = conjunctionClauses;
    }

    @Override
    public void appendTo(final StringBuilder stringBuilder) {
        stringBuilder.append("(");
        joinAndAppend(stringBuilder, conjunctionClauses);
        stringBuilder.append(")");
    }
}

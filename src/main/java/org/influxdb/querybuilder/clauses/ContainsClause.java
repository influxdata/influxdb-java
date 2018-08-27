package org.influxdb.querybuilder.clauses;

public class ContainsClause extends RegexClause {

    public ContainsClause(String name, String value) {
        super(name,"/*"+value+"*/");
    }

}

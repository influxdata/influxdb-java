package org.influxdb.querybuilder;

public class Column {

    private final String name;

    Column(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

}

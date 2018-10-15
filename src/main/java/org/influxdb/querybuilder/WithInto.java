package org.influxdb.querybuilder;

public interface WithInto {

  WithInto into(String measurement);
}

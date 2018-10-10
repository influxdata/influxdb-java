package org.influxdb.impl;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBMapperException;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

public class InfluxDBMapper extends InfluxDBResultMapper {

  private final InfluxDB influxDB;

  public InfluxDBMapper(InfluxDB influxDB) {
    this.influxDB = influxDB;
  }

  public <T> List<T> query(final Query query, final Class<T> clazz) {
    throwExceptionIfMissingAnnotation(clazz);
    QueryResult queryResult = influxDB.query(query);
    return toPOJO(queryResult, clazz);
  }

  public <T> List<T> query(final Class<T> clazz) {
    throwExceptionIfMissingAnnotation(clazz);

    String measurement = getMeasurementName(clazz);
    String database = getDatabaseName(clazz);

    if (database.equals("[unassigned]")) {
      throw new IllegalArgumentException(
          Measurement.class.getSimpleName()
              + " of class "
              + clazz.getName()
              + " should specify a database value for this operation");
    }

    QueryResult queryResult = influxDB.query(new Query("SELECT * FROM " + measurement, database));
    return toPOJO(queryResult, clazz);
  }

  public <T> void save(T model) {
    throwExceptionIfMissingAnnotation(model.getClass());
    cacheMeasurementClass(model.getClass());

    ConcurrentMap<String, Field> colNameAndFieldMap = getColNameAndFieldMap(model.getClass());

    try {

      String measurement = getMeasurementName(model.getClass());
      String database = getDatabaseName(model.getClass());
      TimeUnit timeUnit = getTimeUnit(model.getClass());
      long time = timeUnit.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
      Point.Builder pointBuilder = Point.measurement(measurement).time(time, timeUnit);

      for (String key : colNameAndFieldMap.keySet()) {

        Field field = colNameAndFieldMap.get(key);
        Column column = field.getAnnotation(Column.class);

        String columnName = column.name();
        Object value = field.get(model).toString();

        if (column.tag()) {
          /** Tags are strings either way */
          pointBuilder.tag(columnName, value.toString());
        } else {

          /** Check the primitives, and check what happens on double vs Double */
          if (field.getType().equals(Boolean.TYPE)) {
            pointBuilder.addField(columnName, (boolean) value);
          } else if (field.getType().equals(Long.TYPE)) {
            pointBuilder.addField(columnName, (long) value);
          } else if (field.getType().equals(Double.TYPE)) {
            pointBuilder.addField(columnName, (double) value);
          } else if (field.getType().equals(Number.class)) {
            pointBuilder.addField(columnName, (Number) value);
          } else if (field.getType().equals(String.class)) {
            pointBuilder.addField(columnName, (String) value);
          } else {
            throw new IllegalArgumentException("Argument does not apply");
          }
        }

        influxDB.write(pointBuilder.build());
      }
    } catch (IllegalAccessException e) {
      throw new InfluxDBMapperException(e);
    }
  }
}

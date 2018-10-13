package org.influxdb.impl;

import java.lang.reflect.Field;
import java.time.Instant;
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
  private final long nanoConstant = 1000000000L;

  public InfluxDBMapper(final InfluxDB influxDB) {
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

    if ("[unassigned]".equals(database)) {
      throw new IllegalArgumentException(
          Measurement.class.getSimpleName()
              + " of class "
              + clazz.getName()
              + " should specify a database value for this operation");
    }

    QueryResult queryResult = influxDB.query(new Query("SELECT * FROM " + measurement,database));
    return toPOJO(queryResult, clazz);
  }

  public <T> void save(final T model) {
    throwExceptionIfMissingAnnotation(model.getClass());
    cacheMeasurementClass(model.getClass());

    ConcurrentMap<String, Field> colNameAndFieldMap = getColNameAndFieldMap(model.getClass());

    try {
      Class<?> modelType = model.getClass();
      String measurement = getMeasurementName(modelType);
      String database = getDatabaseName(modelType);
      String retentionPolicy = getRetentionPolicy(modelType);
      TimeUnit timeUnit = getTimeUnit(modelType);
      long time = timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      Point.Builder pointBuilder = Point.measurement(measurement).time(time, timeUnit);

      for (String key : colNameAndFieldMap.keySet()) {
        Field field = colNameAndFieldMap.get(key);
        Column column = field.getAnnotation(Column.class);
        String columnName = column.name();
        Class<?> fieldType = field.getType();

        if (!field.isAccessible()) {
          field.setAccessible(true);
        }

        Object value = field.get(model);

        if (column.tag()) {
          /** Tags are strings either way. */
          pointBuilder.tag(columnName, value.toString());
        } else if ("time".equals(columnName)) {
          if (value != null) {
            setTime(pointBuilder, fieldType, timeUnit, value);
          }
        } else {
          setField(pointBuilder, fieldType, columnName, value);
        }
      }

      Point point = pointBuilder.build();

      if ("[unassigned]".equals(database)) {
        influxDB.write(point);
      } else {
        influxDB.write(database, retentionPolicy, point);
      }

    } catch (IllegalAccessException e) {
      throw new InfluxDBMapperException(e);
    }
  }

  private void setTime(
      final Point.Builder pointBuilder,
      final Class<?> fieldType,
      final TimeUnit timeUnit,
      final Object value) {
    if (Instant.class.isAssignableFrom(fieldType)) {
      Instant instant = (Instant) value;
      long time = timeUnit.convert(instantToNano(instant), TimeUnit.MILLISECONDS);
      pointBuilder.time(time, timeUnit);
    } else {
      throw new InfluxDBMapperException(
          "Unsupported type " + fieldType + " for time: should be of Instant type");
    }
  }

  private void setField(
      final Point.Builder pointBuilder,
      final Class<?> fieldType,
      final String columnName,
      final Object value) {
    if (boolean.class.isAssignableFrom(fieldType) || Boolean.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (boolean) value);
    } else if (long.class.isAssignableFrom(fieldType) || Long.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (long) value);
    } else if (double.class.isAssignableFrom(fieldType)
        || Double.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (double) value);
    } else if (int.class.isAssignableFrom(fieldType) || Integer.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (int) value);
    } else if (String.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (String) value);
    } else {
      throw new InfluxDBMapperException(
          "Unsupported type " + fieldType + " for column " + columnName);
    }
  }

  /**
   * Converts instant to nanoseconds.
   * @param instant
   * @return
   */
  private long instantToNano(final Instant instant) {
    Instant inst = instant.now();
    long time = inst.getEpochSecond();
    time *= nanoConstant;
    time += inst.getNano();
    return time;
  }
}

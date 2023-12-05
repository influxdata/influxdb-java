package org.influxdb.impl;

import org.influxdb.InfluxDB;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.List;

public class InfluxDBMapper extends InfluxDBResultMapper {

  private final InfluxDB influxDB;

  public InfluxDBMapper(final InfluxDB influxDB) {
    this.influxDB = influxDB;
  }

  public <T> List<T> query(final Query query, final Class<T> clazz, final String measurementName) {
    QueryResult queryResult = influxDB.query(query);
    return toPOJO(queryResult, clazz, measurementName);
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

    QueryResult queryResult = influxDB.query(new Query("SELECT * FROM " + measurement, database));
    return toPOJO(queryResult, clazz);
  }

  public <T> void save(final T model) {
    throwExceptionIfMissingAnnotation(model.getClass());
    Class<?> modelType = model.getClass();
    String database = getDatabaseName(modelType);
    String retentionPolicy = getRetentionPolicy(modelType);
    Point.Builder pointBuilder = Point.measurementByPOJO(modelType).addFieldsFromPOJO(model);
    Point point = pointBuilder.build();

    if ("[unassigned]".equals(database)) {
      influxDB.write(point);
    } else {
      influxDB.write(database, retentionPolicy, point);
    }
  }
}

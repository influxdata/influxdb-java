/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 azeti Networks AG (<info@azeti.net>)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.influxdb.impl;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDBMapperException;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.QueryResult;

/**
 * Main class responsible for mapping a QueryResult to a POJO.
 *
 * @author fmachado
 */
public class InfluxDBResultMapper {

  /**
   * Data structure used to cache classes used as measurements.
   */
  private static final
    ConcurrentMap<String, ConcurrentMap<String, Field>> CLASS_FIELD_CACHE = new ConcurrentHashMap<>();

  private static final int FRACTION_MIN_WIDTH = 0;
  private static final int FRACTION_MAX_WIDTH = 9;
  private static final boolean ADD_DECIMAL_POINT = true;

  /**
   * When a query is executed without {@link TimeUnit}, InfluxDB returns the <tt>time</tt>
   * column as an ISO8601 date.
   */
  private static final DateTimeFormatter ISO8601_FORMATTER = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, FRACTION_MIN_WIDTH, FRACTION_MAX_WIDTH, ADD_DECIMAL_POINT)
    .appendPattern("X")
    .toFormatter();

  /**
   * <p>
   * Process a {@link QueryResult} object returned by the InfluxDB client inspecting the internal
   * data structure and creating the respective object instances based on the Class passed as
   * parameter.
   * </p>
   *
   * @param queryResult the InfluxDB result object
   * @param clazz the Class that will be used to hold your measurement data
   * @param <T> the target type
   *
   * @return a {@link List} of objects from the same Class passed as parameter and sorted on the
   * same order as received from InfluxDB.
   *
   * @throws InfluxDBMapperException If {@link QueryResult} parameter contain errors,
   * <tt>clazz</tt> parameter is not annotated with &#64;Measurement or it was not
   * possible to define the values of your POJO (e.g. due to an unsupported field type).
   */
  public <T> List<T> toPOJO(final QueryResult queryResult, final Class<T> clazz) throws InfluxDBMapperException {
    throwExceptionIfMissingAnnotation(clazz);
    String measurementName = getMeasurementName(clazz);
    return this.toPOJO(queryResult, clazz, measurementName);
  }

  /**
   * <p>
   * Process a {@link QueryResult} object returned by the InfluxDB client inspecting the internal
   * data structure and creating the respective object instances based on the Class passed as
   * parameter.
   * </p>
   *
   * @param queryResult the InfluxDB result object
   * @param clazz the Class that will be used to hold your measurement data
   * @param <T> the target type
   * @param measurementName name of the Measurement
   *
   * @return a {@link List} of objects from the same Class passed as parameter and sorted on the
   * same order as received from InfluxDB.
   *
   * @throws InfluxDBMapperException If {@link QueryResult} parameter contain errors,
   * <tt>clazz</tt> parameter is not annotated with &#64;Measurement or it was not
   * possible to define the values of your POJO (e.g. due to an unsupported field type).
   */
  public <T> List<T> toPOJO(final QueryResult queryResult, final Class<T> clazz, final String measurementName)
      throws InfluxDBMapperException {

    Objects.requireNonNull(measurementName, "measurementName");
    Objects.requireNonNull(queryResult, "queryResult");
    Objects.requireNonNull(clazz, "clazz");

    throwExceptionIfResultWithError(queryResult);
    cacheMeasurementClass(clazz);

    List<T> result = new LinkedList<T>();

    queryResult.getResults().stream()
      .filter(internalResult -> Objects.nonNull(internalResult) && Objects.nonNull(internalResult.getSeries()))
      .forEach(internalResult -> {
        internalResult.getSeries().stream()
          .filter(series -> series.getName().equals(measurementName))
          .forEachOrdered(series -> {
            parseSeriesAs(series, clazz, result);
          });
        });

    return result;
  }

  void throwExceptionIfMissingAnnotation(final Class<?> clazz) {
    if (!clazz.isAnnotationPresent(Measurement.class)) {
      throw new IllegalArgumentException(
        "Class " + clazz.getName() + " is not annotated with @" + Measurement.class.getSimpleName());
    }
  }

  void throwExceptionIfResultWithError(final QueryResult queryResult) {
    if (queryResult.getError() != null) {
      throw new InfluxDBMapperException("InfluxDB returned an error: " + queryResult.getError());
    }

    queryResult.getResults().forEach(seriesResult -> {
      if (seriesResult.getError() != null) {
        throw new InfluxDBMapperException("InfluxDB returned an error with Series: " + seriesResult.getError());
      }
    });
  }

  void cacheMeasurementClass(final Class<?>... classVarAgrs) {
    for (Class<?> clazz : classVarAgrs) {
      if (CLASS_FIELD_CACHE.containsKey(clazz.getName())) {
        continue;
      }
      ConcurrentMap<String, Field> initialMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, Field> influxColumnAndFieldMap = CLASS_FIELD_CACHE.putIfAbsent(clazz.getName(), initialMap);
      if (influxColumnAndFieldMap == null) {
        influxColumnAndFieldMap = initialMap;
      }

      for (Field field : clazz.getDeclaredFields()) {
        Column colAnnotation = field.getAnnotation(Column.class);
        if (colAnnotation != null) {
          influxColumnAndFieldMap.put(colAnnotation.name(), field);
        }
      }
    }
  }

  String getMeasurementName(final Class<?> clazz) {
    return ((Measurement) clazz.getAnnotation(Measurement.class)).name();
  }

  <T> List<T> parseSeriesAs(final QueryResult.Series series, final Class<T> clazz, final List<T> result) {
    int columnSize = series.getColumns().size();
    ConcurrentMap<String, Field> colNameAndFieldMap = CLASS_FIELD_CACHE.get(clazz.getName());
    try {
      T object = null;
      for (List<Object> row : series.getValues()) {
        for (int i = 0; i < columnSize; i++) {
          Field correspondingField = colNameAndFieldMap.get(series.getColumns().get(i)/*InfluxDB columnName*/);
          if (correspondingField != null) {
            if (object == null) {
              object = clazz.newInstance();
            }
            setFieldValue(object, correspondingField, row.get(i));
          }
        }
        // When the "GROUP BY" clause is used, "tags" are returned as Map<String,String> and
        // accordingly with InfluxDB documentation
        // https://docs.influxdata.com/influxdb/v1.2/concepts/glossary/#tag-value
        // "tag" values are always String.
        if (series.getTags() != null && !series.getTags().isEmpty()) {
          for (Entry<String, String> entry : series.getTags().entrySet()) {
            Field correspondingField = colNameAndFieldMap.get(entry.getKey()/*InfluxDB columnName*/);
            if (correspondingField != null) {
              // I don't think it is possible to reach here without a valid "object"
              setFieldValue(object, correspondingField, entry.getValue());
            }
          }
        }
        if (object != null) {
          result.add(object);
          object = null;
        }
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new InfluxDBMapperException(e);
    }
    return result;
  }

  /**
   * InfluxDB client returns any number as Double.
   * See https://github.com/influxdata/influxdb-java/issues/153#issuecomment-259681987
   * for more information.
   *
   * @param object
   * @param field
   * @param value
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  <T> void setFieldValue(final T object, final Field field, final Object value)
    throws IllegalArgumentException, IllegalAccessException {
    if (value == null) {
      return;
    }
    Class<?> fieldType = field.getType();
    try {
      if (!field.isAccessible()) {
        field.setAccessible(true);
      }
      if (fieldValueModified(fieldType, field, object, value)
        || fieldValueForPrimitivesModified(fieldType, field, object, value)
        || fieldValueForPrimitiveWrappersModified(fieldType, field, object, value)) {
        return;
      }
      String msg = "Class '%s' field '%s' is from an unsupported type '%s'.";
      throw new InfluxDBMapperException(
        String.format(msg, object.getClass().getName(), field.getName(), field.getType()));
    } catch (ClassCastException e) {
      String msg = "Class '%s' field '%s' was defined with a different field type and caused a ClassCastException. "
        + "The correct type is '%s' (current field value: '%s').";
      throw new InfluxDBMapperException(
        String.format(msg, object.getClass().getName(), field.getName(), value.getClass().getName(), value));
    }
  }

  <T> boolean fieldValueModified(final Class<?> fieldType, final Field field, final T object, final Object value)
    throws IllegalArgumentException, IllegalAccessException {
    if (String.class.isAssignableFrom(fieldType)) {
      field.set(object, String.valueOf(value));
      return true;
    }
    if (Instant.class.isAssignableFrom(fieldType)) {
      Instant instant;
      if (value instanceof String) {
        instant = Instant.from(ISO8601_FORMATTER.parse(String.valueOf(value)));
      } else if (value instanceof Long) {
        instant = Instant.ofEpochMilli((Long) value);
      } else if (value instanceof Double) {
        instant = Instant.ofEpochMilli(((Double) value).longValue());
      } else {
        throw new InfluxDBMapperException("Unsupported type " + field.getClass() + " for field " + field.getName());
      }
      field.set(object, instant);
      return true;
    }
    return false;
  }

  <T> boolean fieldValueForPrimitivesModified(final Class<?> fieldType, final Field field, final T object,
    final Object value) throws IllegalArgumentException, IllegalAccessException {
    if (double.class.isAssignableFrom(fieldType)) {
      field.setDouble(object, ((Double) value).doubleValue());
      return true;
    }
    if (long.class.isAssignableFrom(fieldType)) {
      field.setLong(object, ((Double) value).longValue());
      return true;
    }
    if (int.class.isAssignableFrom(fieldType)) {
      field.setInt(object, ((Double) value).intValue());
      return true;
    }
    if (boolean.class.isAssignableFrom(fieldType)) {
      field.setBoolean(object, Boolean.valueOf(String.valueOf(value)).booleanValue());
      return true;
    }
    return false;
  }

  <T> boolean fieldValueForPrimitiveWrappersModified(final Class<?> fieldType, final Field field, final T object,
    final Object value) throws IllegalArgumentException, IllegalAccessException {
    if (Double.class.isAssignableFrom(fieldType)) {
      field.set(object, value);
      return true;
    }
    if (Long.class.isAssignableFrom(fieldType)) {
      field.set(object, Long.valueOf(((Double) value).longValue()));
      return true;
    }
    if (Integer.class.isAssignableFrom(fieldType)) {
      field.set(object, Integer.valueOf(((Double) value).intValue()));
      return true;
    }
    if (Boolean.class.isAssignableFrom(fieldType)) {
      field.set(object, Boolean.valueOf(String.valueOf(value)));
      return true;
    }
    return false;
  }
}

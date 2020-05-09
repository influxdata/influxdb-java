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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
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

  private static final ConcurrentMap<String, ConcurrentMap<String, Method>> CLASS_SETTERS_CACHE = new ConcurrentHashMap<>();

  private static final int FRACTION_MIN_WIDTH = 0;
  private static final int FRACTION_MAX_WIDTH = 9;
  private static final boolean ADD_DECIMAL_POINT = true;

  /**
   * When a query is executed without {@link TimeUnit}, InfluxDB returns the <code>time</code>
   * column as a RFC3339 date.
   */
  private static final DateTimeFormatter RFC3339_FORMATTER = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, FRACTION_MIN_WIDTH, FRACTION_MAX_WIDTH, ADD_DECIMAL_POINT)
    .appendZoneOrOffsetId()
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
   * <code>clazz</code> parameter is not annotated with &#64;Measurement or it was not
   * possible to define the values of your POJO (e.g. due to an unsupported field type).
   */
  public <T> List<T> toPOJO(final QueryResult queryResult, final Class<T> clazz) throws InfluxDBMapperException {
    return toPOJO(queryResult, clazz, TimeUnit.MILLISECONDS);
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
   * @param precision the time precision of results
   * @param <T> the target type
   *
   * @return a {@link List} of objects from the same Class passed as parameter and sorted on the
   * same order as received from InfluxDB.
   *
   * @throws InfluxDBMapperException If {@link QueryResult} parameter contain errors,
   * <code>clazz</code> parameter is not annotated with &#64;Measurement or it was not
   * possible to define the values of your POJO (e.g. due to an unsupported field type).
   */
  public <T> List<T> toPOJO(final QueryResult queryResult, final Class<T> clazz,
                            final TimeUnit precision) throws InfluxDBMapperException {
    throwExceptionIfMissingAnnotation(clazz);
    String measurementName = getMeasurementName(clazz);
    return this.toPOJO(queryResult, clazz, measurementName, precision);
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
   * <code>clazz</code> parameter is not annotated with &#64;Measurement or it was not
   * possible to define the values of your POJO (e.g. due to an unsupported field type).
   */
  public <T> List<T> toPOJO(final QueryResult queryResult, final Class<T> clazz, final String measurementName)
      throws InfluxDBMapperException {
    return toPOJO(queryResult, clazz, measurementName, TimeUnit.MILLISECONDS);
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
   * @param precision the time precision of results
   *
   * @return a {@link List} of objects from the same Class passed as parameter and sorted on the
   * same order as received from InfluxDB.
   *
   * @throws InfluxDBMapperException If {@link QueryResult} parameter contain errors,
   * <code>clazz</code> parameter is not annotated with &#64;Measurement or it was not
   * possible to define the values of your POJO (e.g. due to an unsupported field type).
   */
  public <T> List<T> toPOJO(final QueryResult queryResult, final Class<T> clazz, final String measurementName,
                            final TimeUnit precision)
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
            parseSeriesAs(series, clazz, result, precision);
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

  ConcurrentMap<String, Field> getColNameAndFieldMap(final Class<?> clazz) {
    return CLASS_FIELD_CACHE.get(clazz.getName());
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

      ConcurrentMap<String, Method> classFieldSetters = new ConcurrentHashMap<>();
      ConcurrentMap<String, Method> fieldSetters = CLASS_SETTERS_CACHE.putIfAbsent(clazz.getName(), classFieldSetters);
      if (fieldSetters == null) {
        fieldSetters = classFieldSetters;
      }

      Class<?> c = clazz;
      while (c != null) {
        for (Field field : c.getDeclaredFields()) {
          Column colAnnotation = field.getAnnotation(Column.class);
          if (colAnnotation != null) {
            String fieldName = field.getName();
            String setterName = "set".concat(fieldName.substring(0,1).toUpperCase().concat(fieldName.substring(1)));
            try {
              Method setter = c.getDeclaredMethod(setterName, field.getType());
              fieldSetters.put(colAnnotation.name(), setter);
            } catch (NoSuchMethodException e) {
              //sj_todo ignore? maybe print a warning that no setter found?
            }
            influxColumnAndFieldMap.put(colAnnotation.name(), field);
          }
        }
        c = c.getSuperclass();
      }
    }
  }

  String getMeasurementName(final Class<?> clazz) {
    return ((Measurement) clazz.getAnnotation(Measurement.class)).name();
  }

  String getDatabaseName(final Class<?> clazz) {
    return ((Measurement) clazz.getAnnotation(Measurement.class)).database();
  }

  String getRetentionPolicy(final Class<?> clazz) {
    return ((Measurement) clazz.getAnnotation(Measurement.class)).retentionPolicy();
  }

  TimeUnit getTimeUnit(final Class<?> clazz) {
    return ((Measurement) clazz.getAnnotation(Measurement.class)).timeUnit();
  }

  <T> List<T> parseSeriesAs(final QueryResult.Series series, final Class<T> clazz, final List<T> result) {
    return parseSeriesAs(series, clazz, result, TimeUnit.MILLISECONDS);
  }

  <T> List<T> parseSeriesAs(final QueryResult.Series series, final Class<T> clazz, final List<T> result,
                            final TimeUnit precision) {
    int columnSize = series.getColumns().size();
    ConcurrentMap<String, Field> colNameAndFieldMap = CLASS_FIELD_CACHE.get(clazz.getName());
    ConcurrentMap<String, Method> fieldSettersMap = CLASS_SETTERS_CACHE.get(clazz.getName());
    try {
      T object = null;
      for (List<Object> row : series.getValues()) {
        for (int i = 0; i < columnSize; i++) {
          Method fieldSetter = fieldSettersMap.get(series.getColumns().get(i));
          Field correspondingField = colNameAndFieldMap.get(series.getColumns().get(i)/*InfluxDB columnName*/);
          if (correspondingField != null || fieldSetter != null) {
            if (object == null) {
              object = clazz.newInstance();
            }
            setFieldValue(object, correspondingField, fieldSetter, row.get(i), precision);
          }
        }
        // When the "GROUP BY" clause is used, "tags" are returned as Map<String,String> and
        // accordingly with InfluxDB documentation
        // https://docs.influxdata.com/influxdb/v1.2/concepts/glossary/#tag-value
        // "tag" values are always String.
        if (series.getTags() != null && !series.getTags().isEmpty()) {
          for (Entry<String, String> entry : series.getTags().entrySet()) {
            Field correspondingField = colNameAndFieldMap.get(entry.getKey()/*InfluxDB columnName*/);
            Method fieldSetter = fieldSettersMap.get(entry.getKey());
            if (correspondingField != null || fieldSetter != null ) {
              // I don't think it is possible to reach here without a valid "object"
              setFieldValue(object, correspondingField, fieldSetter, entry.getValue(), precision);
            }
          }
        }
        if (object != null) {
          result.add(object);
          object = null;
        }
      }
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
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
   * @param precision
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  <T> void setFieldValue(final T object, final Field field, final Method fieldSetter, final Object value, final TimeUnit precision)
          throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    if (value == null) {
      return;
    }
    Class<?> fieldType = field.getType();
    try {
      if (!field.isAccessible()) {
        field.setAccessible(true);
      }
      if (!fieldSetter.isAccessible()) {
        fieldSetter.setAccessible(true);
      }
      if (assignInstant(fieldType, field, fieldSetter, object, value, precision)
        || assignString(fieldType, field, fieldSetter, object, value)
        || assignDouble(fieldType, field, fieldSetter, object, value)
        || assignInteger(fieldType, field, fieldSetter, object, value)
        || assignLong(fieldType, field, fieldSetter, object, value)
        || assignBoolean(fieldType, field, fieldSetter, object, value)) {
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

  <T> boolean assignInstant(final Class<?> fieldType, final Field field, final Method fieldSetter, final T object, final Object value, final TimeUnit precision) throws InvocationTargetException, IllegalAccessException {
    boolean isInstantAssigned = false;
    if(Instant.class.isAssignableFrom(fieldType)) {
      Instant instant;
      if (value instanceof String) {
        instant = Instant.from(RFC3339_FORMATTER.parse(String.valueOf(value)));
      } else if (value instanceof Long) {
        instant = Instant.ofEpochMilli(toMillis((long) value, precision));
      } else if (value instanceof Double) {
        instant = Instant.ofEpochMilli(toMillis(((Double) value).longValue(), precision));
      } else if (value instanceof Integer) {
        instant = Instant.ofEpochMilli(toMillis(((Integer) value).longValue(), precision));
      } else {
        throw new InfluxDBMapperException("Unsupported type " + field.getClass() + " for field " + field.getName());
      }
      if(fieldSetter != null && fieldSetter.isAccessible()) {
        fieldSetter.invoke(object, instant);
        isInstantAssigned = true;
      } else {
        field.set(object, instant);
        isInstantAssigned = true;
      }
    }
    return isInstantAssigned;
  }

  <T> boolean assignString(final Class<?> fieldType, final Field field, final Method fieldSetter, final T object, final Object value) throws InvocationTargetException, IllegalAccessException {
    boolean isStringAssigned = false;
    if(String.class.isAssignableFrom(fieldType)) {
      final String stringValue = String.valueOf(value);
      if(fieldSetter != null && fieldSetter.isAccessible()) {
        fieldSetter.invoke(object, stringValue);
        isStringAssigned = true;
      } else {
        field.set(object, stringValue);
        isStringAssigned = true;
      }
    }
    return isStringAssigned;
  }

  <T> boolean assignDouble(final Class<?> fieldType, final Field field, final Method fieldSetter, final T object, final Object value) throws InvocationTargetException, IllegalAccessException {
    boolean isDoubleAssigned = false;
    if(double.class.isAssignableFrom(fieldType) || Double.class.isAssignableFrom(fieldType)) {
      if(fieldSetter != null && fieldSetter.isAccessible()) {
        fieldSetter.invoke(object, value);
        isDoubleAssigned = true;
      } else if(double.class.isAssignableFrom(fieldType)) {
        final double doubleValue = (Double) value;
        field.setDouble(object, doubleValue);
        isDoubleAssigned = true;
      } else if(Double.class.isAssignableFrom(fieldType)) {
        field.set(object, value);
        isDoubleAssigned = true;
      }
    }
    return isDoubleAssigned;
  }

  <T> boolean assignLong(final Class<?> fieldType, final Field field, final Method fieldSetter, final T object, final Object value) throws InvocationTargetException, IllegalAccessException {
    boolean isLongAssigned = false;
    if(long.class.isAssignableFrom(fieldType) || Long.class.isAssignableFrom(fieldType)) {
      final long longValue = ((Double) value).longValue();
      if(fieldSetter != null && fieldSetter.isAccessible()) {
        fieldSetter.invoke(object, longValue);
        isLongAssigned = true;
      } else if(long.class.isAssignableFrom(fieldType)) {
        field.setLong(object, longValue);
        isLongAssigned = true;
      } else if(Long.class.isAssignableFrom(fieldType)) {
        field.set(object, longValue);
        isLongAssigned = true;
      }
    }
    return isLongAssigned;
  }

  <T> boolean assignInteger(final Class<?> fieldType, final Field field, final Method fieldSetter, final T object, final Object value) throws InvocationTargetException, IllegalAccessException {
    boolean isIntegerAssigned = false;
    if(int.class.isAssignableFrom(fieldType) || Integer.class.isAssignableFrom(fieldType)) {
      final int intValue = ((Double) value).intValue();
      if(fieldSetter != null && fieldSetter.isAccessible()) {
        fieldSetter.invoke(object, intValue);
        isIntegerAssigned = true;
      } else if(int.class.isAssignableFrom(fieldType)) {
        field.setInt(object, intValue);
        isIntegerAssigned = true;
      } else if(Integer.class.isAssignableFrom(fieldType)) {
        field.set(object, intValue);
        isIntegerAssigned = true;
      }
    }
    return isIntegerAssigned;
  }

  <T> boolean assignBoolean(final Class<?> fieldType, final Field field, final Method fieldSetter, final T object, final Object value) throws InvocationTargetException, IllegalAccessException {
    boolean isBooleanAssigned = false;
    if(boolean.class.isAssignableFrom(fieldType) || Boolean.class.isAssignableFrom(fieldType)) {
      final boolean boolValue = Boolean.parseBoolean(String.valueOf(value));
      if(fieldSetter != null && fieldSetter.isAccessible()) {
        fieldSetter.invoke(object, boolValue);
        isBooleanAssigned = true;
      } else if(boolean.class.isAssignableFrom(fieldType)) {
        field.setBoolean(object, boolValue);
        isBooleanAssigned = true;
      } else if(Boolean.class.isAssignableFrom(fieldType)) {
        field.set(object, boolValue);
        isBooleanAssigned = true;
      }
    }
    return isBooleanAssigned;
  }

  private Long toMillis(final long value, final TimeUnit precision) {

    return TimeUnit.MILLISECONDS.convert(value, precision);
  }
}

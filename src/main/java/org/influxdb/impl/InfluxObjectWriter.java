package org.influxdb.impl;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;

public class InfluxObjectWriter {


  public Point write(final String database, final Object object) {

    throwExceptionIfMissingAnnotation(object.getClass());
    String measurementName = getMeasurementName(object.getClass());


    Builder builder =
        Point.measurement(measurementName).time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    Map<String, Object> fields = getFieldList(object);

    builder.fields(fields);

    return builder.build();
  }

  private String getMeasurementName(final Class<?> clazz) {
    return clazz.getAnnotation(Measurement.class).name();
  }

  private Map<String, Object> getFieldList(final Object object) {


    Map<String, Object> fields = new HashMap<>();


    for (Field field : object.getClass().getFields()) {

      Column column = field.getAnnotation(Column.class);

      String fieldName = column.name();

      try {

        Object fieldValue = field.get(object);

        fields.put(fieldName, fieldValue);

      } catch (IllegalArgumentException | IllegalAccessException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }


      // }
    }


    return fields;
  }


  private void throwExceptionIfMissingAnnotation(final Class<?> clazz) {
    if (!clazz.isAnnotationPresent(Measurement.class)) {
      throw new IllegalArgumentException("Class " + clazz.getName() + " is not annotated with @"
          + Measurement.class.getSimpleName());
    }
  }

}

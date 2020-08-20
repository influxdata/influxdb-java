package org.influxdb.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(value = ElementType.FIELD)
public @interface Default {
  String value() default "";
  long longValue() default 0L;
  int intValue() default 0;
  double doubleValue() default 0.0d;
  float floatValue() default 0.0f;
  boolean boolValue() default false;
}

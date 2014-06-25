package org.influxdb.impl;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import retrofit.http.RestMethod;

/**
 * Make a DELETE with Body request to a REST path relative to base URL.
 * 
 * @see "https://github.com/square/retrofit/issues/330"
 * 
 *      why this is necessary.
 */
@Documented
@Target(METHOD)
@Retention(RUNTIME)
@RestMethod(value = "DELETE", hasBody = true)
public @interface DELETE {
	/**
	 * @return the value of the path.
	 */
	String value();
}
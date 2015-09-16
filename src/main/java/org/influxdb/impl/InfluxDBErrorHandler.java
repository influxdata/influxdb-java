package org.influxdb.impl;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit.ErrorHandler;
import retrofit.RetrofitError;
import retrofit.client.Response;

import java.io.IOException;
import java.io.InputStreamReader;

class InfluxDBErrorHandler implements ErrorHandler {
   private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBErrorHandler.class);

	@Override
	public Throwable handleError(final RetrofitError cause) {
		Response r = cause.getResponse();
		if (r != null && r.getStatus() >= 400) {
			try (InputStreamReader reader = new InputStreamReader(r.getBody().in(), Charsets.UTF_8)) {
            String message = CharStreams.toString(reader);
            LOGGER.debug("Error message {}", message);
            return new RuntimeException(message);
			} catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
			}
		}
		return cause;
	}
}

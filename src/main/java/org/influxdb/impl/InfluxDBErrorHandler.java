package org.influxdb.impl;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit.ErrorHandler;
import retrofit.RetrofitError;
import retrofit.client.Response;

import java.io.IOException;
import java.io.InputStreamReader;

import static java.lang.String.format;

class InfluxDBErrorHandler implements ErrorHandler {
   private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBErrorHandler.class);
   private static final String FORMAT = "Body : %s, reason : %s, code : %d";

	@Override
	public Throwable handleError(RetrofitError retrofitError) {
      Response r = retrofitError.getResponse();
      if (r != null && r.getStatus() >= 400) {
         try (InputStreamReader reader = new InputStreamReader(r.getBody().in(), Charsets.UTF_8)) {
            return new RuntimeException(format(FORMAT, CharStreams.toString(reader), r.getReason(), r.getStatus()));
         }
         catch (IOException e) {
            LOGGER.error("Filed reading error body {}", r);
            throw Throwables.propagate(e);
         }
      }
      return retrofitError;
	}
}

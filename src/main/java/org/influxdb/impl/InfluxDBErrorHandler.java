package org.influxdb.impl;

import java.io.IOException;
import java.io.InputStreamReader;

import com.google.common.io.Closeables;
import retrofit.ErrorHandler;
import retrofit.RetrofitError;
import retrofit.client.Response;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

class InfluxDBErrorHandler implements ErrorHandler {
	@Override
	public Throwable handleError(final RetrofitError cause) {
		Response r = cause.getResponse();
		if (r != null && r.getStatus() >= 400) {
            InputStreamReader reader = null;
            try {
                reader = new InputStreamReader(r.getBody().in(), Charsets.UTF_8);
                return new RuntimeException(CharStreams.toString(reader));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                Closeables.closeQuietly(reader);
            }
        }
		return cause;
	}
}

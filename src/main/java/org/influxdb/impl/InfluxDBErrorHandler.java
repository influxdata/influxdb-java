package org.influxdb.impl;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import retrofit.ErrorHandler;
import retrofit.RetrofitError;
import retrofit.client.Response;

class InfluxDBErrorHandler implements ErrorHandler {
	@Override
	public Throwable handleError(final RetrofitError cause) {
		switch (cause.getKind()) {
		case CONVERSION:
			break;
		case HTTP:
			Response r = cause.getResponse();
			
			if ((r != null) && (r.getBody() != null)) {
				try {
					InputStreamReader reader = new InputStreamReader(r.getBody().in(), Charsets.UTF_8);
					Exception e = new RuntimeException(CharStreams.toString(reader));
					reader.close();
					return e;					
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			break;
		case NETWORK:
			
			if(cause.getMessage().contains("authentication")){
                //401 errors
                return new Exception("Invalid credentials. Please verify login info.");
            }else if (cause.getCause() instanceof SocketTimeoutException) {
                //Socket Timeout
                return new SocketTimeoutException("Connection Timeout. " +
                        "Please verify your internet connection.");
            } else {
                //No Connection
                return new ConnectException("No Connection. " +
                        "Please verify your internet connection.");
            }
//			break;
		case UNEXPECTED:
			break;
		default:
			break;
		}
		
		return cause;
	}
}

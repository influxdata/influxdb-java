package org.influxdb.dto;

import retrofit.RetrofitError;
import retrofit.client.Response;

public final class CustomResponse<T> {
	private final RetrofitError error;
	private final Response response;
	private final T result;
	
	CustomResponse(RetrofitError error, Response response, T result) {
		this.error = error;
		this.response = response;
		this.result = result;
	}
	
	public CustomResponse(Response response, T t) {
		this(null, response, t);
	}
	
	public CustomResponse(T t) {
		this(null, null, t);
	}
	
	public CustomResponse(RetrofitError error) {
		this(error, null, null);
	}
	
	public boolean hasError() {
		return error != null;
	}

	public RetrofitError getError() {
		return error;
	}

	public Response getResponse() {
		return response;
	}

	public T getResult() {
		return result;
	}
}

package org.influxdb.dto;

import retrofit.Callback;
import retrofit.RetrofitError;
import retrofit.client.Response;

public class CustomCallbackImpl<T> implements Callback<T> {
	private final CustomCallback<T> callback;
	
	public CustomCallbackImpl(CustomCallback<T> callback) {
		this.callback = callback;
	}

	@Override
	public final void success(T t, Response response) {
		callback.onComplete(new CustomResponse<>(response, t));
	}

	@Override
	public final void failure(RetrofitError error) {
		callback.onComplete(new CustomResponse<>(error));
	}
}

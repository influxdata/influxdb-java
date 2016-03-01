package org.influxdb.dto;

public interface CustomCallback<T> {
	public void onComplete(CustomResponse<T> response);
}
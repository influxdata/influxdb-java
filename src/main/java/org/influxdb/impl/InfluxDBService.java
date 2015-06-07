package org.influxdb.impl;

import org.influxdb.dto.QueryResult;
import org.influxdb.dto.BatchPoints;

import retrofit.Callback;
import retrofit.client.Response;
import retrofit.http.Body;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.Query;

interface InfluxDBService {

	public static final String U = "u";
	public static final String P = "p";
	public static final String Q = "q";
	public static final String DB = "db";

	@GET("/ping")
	public Response ping();

	@POST("/write")
	public String batchPoints(@Query(U) String username, @Query(P) String password, @Body BatchPoints batchPoints);

	@POST("/write")
	public void batchPoints(@Query(U) String username, @Query(P) String password, @Body BatchPoints batchPoints,
			Callback<String> cb);

	@GET("/query")
	public QueryResult query(@Query(U) String username, @Query(P) String password, @Query(DB) String db,
			@Query(Q) String query);

	@GET("/query")
	public QueryResult query(@Query(U) String username, @Query(P) String password, @Query(Q) String query);

}

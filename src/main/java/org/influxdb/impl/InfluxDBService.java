package org.influxdb.impl;

import java.util.List;

import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Serie;
import org.influxdb.dto.User;

import retrofit.http.Body;
import retrofit.http.DELETE;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.Path;
import retrofit.http.Query;

interface InfluxDBService {

	@GET("/ping")
	public Pong ping();

	@POST("/db")
	public String createDatabase(@Body Database database, @Query("u") String username, @Query("p") String password);

	@DELETE("/db/{name}")
	public String deleteDatabase(@Path("name") String name, @Query("u") String username, @Query("p") String password);

	@GET("/db")
	public List<Database> describeDatabases(@Query("u") String username, @Query("p") String password);

	@POST("/db/{name}/series")
	public String write(@Path("name") String name, @Body Serie[] series, @Query("u") String username,
			@Query("p") String password, @Query("time_precision") String timePrecision);

	@GET("/db/{name}/series")
	public List<Serie> query(@Path("name") String name, @Query("q") String query, @Query("u") String username,
			@Query("p") String password, @Query("time_precision") String timePrecision);

	@POST("/cluster_admins")
	public String createClusterAdmin(@Body User user, @Query("u") String username, @Query("p") String password);

	@DELETE("/cluster_admins/{name}")
	public String deleteClusterAdmin(@Path("name") String name, @Query("u") String username, @Query("p") String password);

	@GET("/cluster_admins")
	public List<User> describeClusterAdmins(@Query("u") String username, @Query("p") String password);

	@POST("/cluster_admins/{name}")
	public String updateClusterAdmin(@Body User user, @Path("name") String name, @Query("u") String username,
			@Query("p") String password);

	@POST("/db/{database}/users")
	public String createDatabaseUser(@Path("database") String database, @Body User user, @Query("u") String username,
			@Query("p") String password);

	@DELETE("/db/{database}/users/{name}")
	public String deleteDatabaseUser(@Path("database") String database, @Path("name") String name,
			@Query("u") String username, @Query("p") String password);

	@GET("/db/{database}/users")
	public List<User> describeDatabaseUsers(@Path("database") String database, @Query("u") String username,
			@Query("p") String password);

	@POST("/db/{database}/users/{name}")
	public String updateDatabaseUser(@Path("database") String database, @Body User user, @Path("name") String name,
			@Query("u") String username, @Query("p") String password);

	@GET("/db/{database}/authenticate")
	public String authenticateDatabaseUser(@Path("database") final String database, @Query("u") String username,
			@Query("p") String password);

	@GET("/db/{database}/continuous_queries")
	public List<ContinuousQuery> getContinuousQueries(@Path("database") String database, @Query("u") String username,
			@Query("p") String password);

	@DELETE("/db/{database}/continuous_queries/{id}")
	public String deleteContinuousQuery(@Path("database") String database, @Path("id") int id,
			@Query("u") String username, @Query("p") String password);

}

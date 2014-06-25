package org.influxdb.impl;

import java.util.List;

import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Serie;
import org.influxdb.dto.Server;
import org.influxdb.dto.Shard;
import org.influxdb.dto.Shard.Member;
import org.influxdb.dto.Shards;
import org.influxdb.dto.User;

import retrofit.client.Response;
import retrofit.http.Body;
import retrofit.http.DELETE;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.Path;
import retrofit.http.Query;

interface InfluxDBService {

	@GET("/ping")
	public Pong ping();

	@GET("/interfaces")
	public Response version();

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

	@DELETE("/db/{database}/series/{name}")
	public String deletePoints(@Path("database") final String database, @Path("name") String name,
			@Query("u") String username, @Query("p") String password);

	// TODO new methods start here.
	@POST("/raft/force_compaction")
	public String forceRaftCompaction(@Query("u") String username, @Query("p") String password);

	@GET("/interfaces")
	public List<String> interfaces(@Query("u") String username, @Query("p") String password);

	@GET("/sync")
	public Boolean sync(@Query("u") String username, @Query("p") String password);

	@GET("/cluster/servers")
	public List<Server> listServers(@Query("u") String username, @Query("p") String password);

	@DELETE("/cluster/servers/{id}")
	public String removeServers(@Path("id") int id, @Query("u") String username, @Query("p") String password);

	@POST("/cluster/shards")
	public String createShard(@Query("u") String username, @Query("p") String password, @Body Shard shard);

	@GET("/cluster/shards")
	public Shards getShards(@Query("u") String username, @Query("p") String password);

	@org.influxdb.impl.DELETE("/cluster/shards/{id}")
	public String dropShard(@Path("id") int id, @Query("u") String username, @Query("p") String password,
			@Body Member servers);

}

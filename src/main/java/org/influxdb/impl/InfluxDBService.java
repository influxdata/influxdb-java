package org.influxdb.impl;

import java.util.List;

import org.influxdb.dto.ContinuousQuery;
import org.influxdb.dto.Database;
import org.influxdb.dto.DatabaseConfiguration;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Serie;
import org.influxdb.dto.Server;
import org.influxdb.dto.Shard;
import org.influxdb.dto.Shard.Member;
import org.influxdb.dto.ShardSpace;
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

	public static final String U = "u";
	public static final String P = "p";
	public static final String Q = "q";
	public static final String ID = "id";
	public static final String NAME = "name";
	public static final String DATABASE = "database";

	@GET("/ping")
	public Pong ping();

	@GET("/interfaces")
	public Response version();

	@POST("/db")
	public String createDatabase(@Body Database database, @Query(U) String username, @Query(P) String password);

	@POST("/cluster/database_configs/{name}")
	public String createDatabase(@Path(NAME) String name, @Body DatabaseConfiguration config,
			@Query(U) String username, @Query(P) String password);

	@DELETE("/db/{name}")
	public String deleteDatabase(@Path(NAME) String name, @Query(U) String username, @Query(P) String password);

	@GET("/db")
	public List<Database> describeDatabases(@Query(U) String username, @Query(P) String password);

	@POST("/db/{name}/series")
	public String write(@Path(NAME) String name, @Body Serie[] series, @Query(U) String username,
			@Query(P) String password, @Query("time_precision") String timePrecision);

	@GET("/db/{name}/series")
	public List<Serie> query(@Path(NAME) String name, @Query(Q) String query, @Query(U) String username,
			@Query(P) String password, @Query("time_precision") String timePrecision);

	@POST("/cluster_admins")
	public String createClusterAdmin(@Body User user, @Query(U) String username, @Query(P) String password);

	@DELETE("/cluster_admins/{name}")
	public String deleteClusterAdmin(@Path(NAME) String name, @Query(U) String username, @Query(P) String password);

	@GET("/cluster_admins")
	public List<User> describeClusterAdmins(@Query(U) String username, @Query(P) String password);

	@POST("/cluster_admins/{name}")
	public String updateClusterAdmin(@Body User user, @Path(NAME) String name, @Query(U) String username,
			@Query(P) String password);

	@POST("/db/{database}/users")
	public String createDatabaseUser(@Path(DATABASE) String database, @Body User user, @Query(U) String username,
			@Query(P) String password);

	@DELETE("/db/{database}/users/{name}")
	public String deleteDatabaseUser(@Path(DATABASE) String database, @Path(NAME) String name,
			@Query(U) String username, @Query(P) String password);

	@GET("/db/{database}/users")
	public List<User> describeDatabaseUsers(@Path(DATABASE) String database, @Query(U) String username,
			@Query(P) String password);

	@POST("/db/{database}/users/{name}")
	public String updateDatabaseUser(@Path(DATABASE) String database, @Body User user, @Path(NAME) String name,
			@Query(U) String username, @Query(P) String password);

	@GET("/db/{database}/authenticate")
	public String authenticateDatabaseUser(@Path(DATABASE) final String database, @Query(U) String username,
			@Query(P) String password);

	@GET("/db/{database}/continuous_queries")
	public List<ContinuousQuery> getContinuousQueries(@Path(DATABASE) String database, @Query(U) String username,
			@Query(P) String password);

	@DELETE("/db/{database}/continuous_queries/{id}")
	public String deleteContinuousQuery(@Path(DATABASE) String database, @Path(ID) int id, @Query(U) String username,
			@Query(P) String password);

	@DELETE("/db/{database}/series/{name}")
	public String deleteSeries(@Path(DATABASE) final String database, @Path(NAME) String name,
			@Query(U) String username, @Query(P) String password);

	@POST("/raft/force_compaction")
	public String forceRaftCompaction(@Query(U) String username, @Query(P) String password);

	@GET("/interfaces")
	public List<String> interfaces(@Query(U) String username, @Query(P) String password);

	@GET("/sync")
	public Boolean sync(@Query(U) String username, @Query(P) String password);

	@GET("/cluster/servers")
	public List<Server> listServers(@Query(U) String username, @Query(P) String password);

	@DELETE("/cluster/servers/{id}")
	public String removeServers(@Path(ID) int id, @Query(U) String username, @Query(P) String password);

	@POST("/cluster/shards")
	public String createShard(@Query(U) String username, @Query(P) String password, @Body Shard shard);

	@GET("/cluster/shards")
	public Shards getShards(@Query(U) String username, @Query(P) String password);

	@org.influxdb.impl.DELETE("/cluster/shards/{id}")
	public String dropShard(@Path(ID) int id, @Query(U) String username, @Query(P) String password, @Body Member servers);

	@GET("/cluster/shard_spaces")
	public List<ShardSpace> getShardSpaces(@Query(U) String username, @Query(P) String password);

	@org.influxdb.impl.DELETE("/cluster/shard_spaces/{database}/{name}")
	public String dropShardSpace(@Path(DATABASE) String database, @Path(NAME) String name, @Query(U) String username,
			@Query(P) String password);

	@POST("/cluster/shard_spaces/{database}")
	public String createShardSpace(@Path(DATABASE) String database, @Query(U) String username,
			@Query(P) String password, @Body ShardSpace shardSpace);

}

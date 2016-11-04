package org.influxdb.impl;


import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.UdpBatchPoints;
import org.influxdb.impl.BatchProcessor.AbstractBatchEntry;
import org.influxdb.impl.BatchProcessor.BatchEntry;
import org.influxdb.impl.BatchProcessor.UdpBatchEntry;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.logging.HttpLoggingInterceptor.Level;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.moshi.MoshiConverterFactory;

/**
 * Implementation of a InluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 */
public class InfluxDBImpl implements InfluxDB {
	static final okhttp3.MediaType MEDIA_TYPE_STRING = MediaType.parse("text/plain");

	private final String username;
	private final String password;
	private final Retrofit retrofit;
	private final InfluxDBService influxDBService;
	private final String url;
	private BatchProcessor batchProcessor;
	private final AtomicBoolean batchEnabled = new AtomicBoolean(false);
	private final AtomicLong writeCount = new AtomicLong();
	private final AtomicLong unBatchedCount = new AtomicLong();
	private final AtomicLong batchedCount = new AtomicLong();
	private final HttpLoggingInterceptor loggingInterceptor;
	private LogLevel logLevel = LogLevel.NONE;

	public InfluxDBImpl(final String url, final String username, final String password,
			final OkHttpClient.Builder client) {
		super();
		this.url = url;
		this.username = username;
		this.password = password;
		this.loggingInterceptor = new HttpLoggingInterceptor();
		this.loggingInterceptor.setLevel(Level.NONE);
		this.retrofit = new Retrofit.Builder()
				.baseUrl(url)
				.client(client.addInterceptor(loggingInterceptor).build())
				.addConverterFactory(MoshiConverterFactory.create())
				.build();
		this.influxDBService = this.retrofit.create(InfluxDBService.class);
	}

	@Override
	public InfluxDB setLogLevel(final LogLevel logLevel) {
		switch (logLevel) {
		case NONE:
			this.loggingInterceptor.setLevel(Level.NONE);
			break;
		case BASIC:
			this.loggingInterceptor.setLevel(Level.BASIC);
			break;
		case HEADERS:
			this.loggingInterceptor.setLevel(Level.HEADERS);
			break;
		case FULL:
			this.loggingInterceptor.setLevel(Level.BODY);
			break;
		default:
			break;
		}
		this.logLevel = logLevel;
		return this;
	}

	@Override
	public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit)	{
		if (this.batchEnabled.get()) {
			throw new IllegalArgumentException("BatchProcessing is already enabled.");
		}
		this.batchProcessor = BatchProcessor
				.builder(this)
				.actions(actions)
				.interval(flushDuration, flushDurationTimeUnit)
				.build();
		this.batchEnabled.set(true);
		return this;
	}

	@Override
	public void disableBatch() {
		this.batchEnabled.set(false);
		if(this.batchProcessor != null) {
			this.batchProcessor.flush();
			if (this.logLevel != LogLevel.NONE) {
				System.out.println(
						"total writes:" + this.writeCount.get() + " unbatched:" + this.unBatchedCount.get() + "batchPoints:"
								+ this.batchedCount);
			}
		}
	}

	@Override
	public boolean isBatchEnabled()	{
		return this.batchEnabled.get();
	}

	@Override
	public Pong ping() {
		Stopwatch watch = Stopwatch.createStarted();
		Call<ResponseBody> call = this.influxDBService.ping();
		try {
			Response<ResponseBody> response = call.execute();
			Headers headers = response.headers();
			String version = "unknown";
			for (String name : headers.toMultimap().keySet()) {
				if (null != name && name.equalsIgnoreCase("X-Influxdb-Version")) {
					version = headers.get(name);
					break;
				}
			}
			Pong pong = new Pong();
			pong.setVersion(version);
			pong.setResponseTime(watch.elapsed(TimeUnit.MILLISECONDS));
			return pong;
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

	@Override
	public String version()	{
		return ping().getVersion();
	}
	
	@Override
	public void write(final int udpPort, final Point point) {
		if (this.batchEnabled.get()) {
			AbstractBatchEntry batchEntry = new UdpBatchEntry(point, udpPort);
			this.batchProcessor.put(batchEntry);
		} else {
			UdpBatchPoints batchPoints = UdpBatchPoints.udpPort(udpPort).build();
			batchPoints.point(point);
			this.write(batchPoints);
			this.unBatchedCount.incrementAndGet();
		}
		this.writeCount.incrementAndGet();
	}
	
	/* 
	 * TODO to cache udp socket by port
	 * FIXME　url should be host instead of http
	 */
	@Override
	public void write(final UdpBatchPoints batchPoints) {
		this.batchedCount.addAndGet(batchPoints.getPoints().size());
		String lineProtocol = batchPoints.lineProtocol();
		int udpPort = batchPoints.getUdpPort();
		DatagramSocket datagramSocket = null;
		try {
			datagramSocket = new DatagramSocket( new InetSocketAddress(url, udpPort));
			byte[] bytes = lineProtocol.getBytes(Charsets.UTF_8);
			datagramSocket.send(new DatagramPacket(bytes, bytes.length));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(datagramSocket!=null){
				datagramSocket.close();
			}
		}
	}

	@Override
	public void write(final String database, final String retentionPolicy, final Point point) {
		if (this.batchEnabled.get()) {
			BatchEntry batchEntry = new BatchEntry(point, database, retentionPolicy);
			this.batchProcessor.put(batchEntry);
		} else {
			BatchPoints batchPoints = BatchPoints.database(database).retentionPolicy(retentionPolicy).build();
			batchPoints.point(point);
			this.write(batchPoints);
			this.unBatchedCount.incrementAndGet();
		}
		this.writeCount.incrementAndGet();
	}

	@Override
	public void write(final BatchPoints batchPoints) {
		this.batchedCount.addAndGet(batchPoints.getPoints().size());
		RequestBody lineProtocol = RequestBody.create(MEDIA_TYPE_STRING, batchPoints.lineProtocol());
		execute(this.influxDBService.writePoints(
				this.username,
				this.password,
				batchPoints.getDatabase(),
				batchPoints.getRetentionPolicy(),
				TimeUtil.toTimePrecision(TimeUnit.NANOSECONDS),
				batchPoints.getConsistency().value(),
				lineProtocol));
	}

	@Override
	public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
			final String records)	{
		execute(this.influxDBService.writePoints(
				this.username,
				this.password,
				database,
				retentionPolicy,
				TimeUtil.toTimePrecision(TimeUnit.NANOSECONDS),
				consistency.value(),
				RequestBody.create(MEDIA_TYPE_STRING, records)));
	}

	@Override
	public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
			final List<String> records)	{
		final String joinedRecords = Joiner.on("\n").join(records);
		write(database, retentionPolicy, consistency, joinedRecords);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryResult query(final Query query)	{
		Call<QueryResult> call;
		if (query.requiresPost()) {
			call = this.influxDBService.postQuery(this.username, this.password, query.getDatabase(), query.getCommand());
		} else {
			call = this.influxDBService.query(this.username, this.password, query.getDatabase(), query.getCommand());
		}
		return execute(call);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryResult query(final Query query, final TimeUnit timeUnit) {
		return execute(this.influxDBService.query(this.username, this.password, query.getDatabase(),
				TimeUtil.toTimePrecision(timeUnit), query.getCommand()));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createDatabase(final String name) {
		Preconditions.checkArgument(!name.contains("-"), "Database name cant contain -");
		String createDatabaseQueryString = String.format("CREATE DATABASE \"%s\"", name);
		if (this.version().startsWith("0.")) {
			createDatabaseQueryString = String.format("CREATE DATABASE IF NOT EXISTS \"%s\"", name);
		}
		execute(this.influxDBService.postQuery(this.username, this.password, createDatabaseQueryString));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteDatabase(final String name)	{
		execute(this.influxDBService.postQuery(this.username, this.password, "DROP DATABASE \"" + name + "\""));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> describeDatabases()	{
		QueryResult result = execute(this.influxDBService.query(this.username, this.password, "SHOW DATABASES"));
		// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["mydb"]]}]}]}
		// Series [name=databases, columns=[name], values=[[mydb], [unittest_1433605300968]]]
		List<List<Object>> databaseNames = result.getResults().get(0).getSeries().get(0).getValues();
		List<String> databases = Lists.newArrayList();
		if (databaseNames != null) {
			for (List<Object> database : databaseNames) {
				databases.add(database.get(0).toString());
			}
		}
		return databases;
	}

	private <T> T execute(Call<T> call)	{
		try {
			Response<T> response = call.execute();
			if (response.isSuccessful()) {
				return response.body();
			}
			try (ResponseBody errorBody = response.errorBody()){
				throw new RuntimeException(errorBody.string());
			}
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

}

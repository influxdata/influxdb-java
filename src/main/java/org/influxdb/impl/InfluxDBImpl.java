package org.influxdb.impl;

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
import org.influxdb.impl.BatchProcessor.BatchEntry;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.squareup.okhttp.OkHttpClient;

import retrofit.RestAdapter;
import retrofit.client.Client;
import retrofit.client.Header;
import retrofit.client.OkClient;
import retrofit.client.Response;
import retrofit.mime.TypedString;

/**
 * Implementation of a InluxDB API.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class InfluxDBImpl implements InfluxDB {
	private final String username;
	private final String password;
	private final RestAdapter restAdapter;
	private final InfluxDBService influxDBService;
	private BatchProcessor batchProcessor;
	private final AtomicBoolean batchEnabled = new AtomicBoolean(false);
	private final AtomicLong writeCount = new AtomicLong();
	private final AtomicLong unBatchedCount = new AtomicLong();
	private final AtomicLong batchedCount = new AtomicLong();
	private LogLevel logLevel = LogLevel.NONE;

	/**
	 * Constructor which should only be used from the InfluxDBFactory.
	 * 
	 * @param url
	 *            the url where the influxdb is accessible.
	 * @param username
	 *            the user to connect.
	 * @param password
	 *            the password for this user.
	 */
	public InfluxDBImpl(final String url, final String username, final String password) {
		super();
		this.username = username;
		this.password = password;
		Client client = new OkClient(new OkHttpClient());
		restAdapter = new RestAdapter.Builder()
				.setEndpoint(url)
				.setErrorHandler(new InfluxDBErrorHandler())
				.setClient(client)
				.build();
		influxDBService = restAdapter.create(InfluxDBService.class);
	}

	@Override
	public InfluxDB setLogLevel(final LogLevel logLevel) {
		switch (logLevel) {
		case NONE:
			restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.NONE);
			break;
		case BASIC:
			restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.BASIC);
			break;
		case HEADERS:
			restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.HEADERS);
			break;
		case FULL:
			restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.FULL);
			break;
		default:
			break;
		}
		this.logLevel = logLevel;
		return this;
	}

	@Override
	public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit) {
		if (batchEnabled.get()) {
			throw new IllegalArgumentException("BatchProcessing is already enabled.");
		}
		batchProcessor = BatchProcessor
				.builder(this)
				.actions(actions)
				.interval(flushDuration, flushDurationTimeUnit)
				.build();
		batchEnabled.set(true);
		return this;
	}

	@Override
	public void disableBatch() {
		batchEnabled.set(false);
		batchProcessor.flush();
		if (logLevel != LogLevel.NONE) {
			System.out.println(String.format("Total writes:%d Unbatched:%d Batched:%d",
					writeCount.get(), unBatchedCount.get(), batchedCount.get()));
		}
	}

	@Override
	public Pong ping() {
		Stopwatch watch = Stopwatch.createStarted();
		Response response = influxDBService.ping();
		List<Header> headers = response.getHeaders();
		String version = "unknown";
		for (Header header : headers) {
			if (null != header.getName() && header.getName().equalsIgnoreCase("X-Influxdb-Version")) {
				version = header.getValue();
			}
		}
		Pong pong = new Pong();
		pong.setVersion(version);
		pong.setResponseTime(watch.elapsed(TimeUnit.MILLISECONDS));
		return pong;
	}

	@Override
	public String version() {
		return ping().getVersion();
	}

	@Override
	public void write(final String database, final String retentionPolicy, final Point point) {
		write(database, retentionPolicy, point);
	}

	@Override
	public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistencyLevel, final Point point) {
		if (batchEnabled.get()) {
			BatchEntry batchEntry = new BatchEntry(point, database, consistencyLevel, retentionPolicy);
			batchProcessor.put(batchEntry);
		} else {
			writeUnbatched(database, retentionPolicy, ConsistencyLevel.ONE, point);
		}
	}
	
	@Override
	public void write(BatchPoints batchPoints) {
		write(batchPoints.getDatabase(), batchPoints.getRetentionPolicy(), ConsistencyLevel.ONE, batchPoints.getPoints());
	}
	
	@Override
	public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistencyLevel, final List<Point> points) {
		writeBatched(database, retentionPolicy, consistencyLevel, points);
	}
	
	protected void writeBatched(final String database, final String retentionPolicy, final ConsistencyLevel consistencyLevel, final List<Point> points) {
		batchedCount.addAndGet(points.size());
		writeCount.addAndGet(points.size());
		writeLine(database, retentionPolicy, consistencyLevel, Point.toLineProtocol(points));
	}

	protected void writeUnbatched(String database, String retentionPolicy, ConsistencyLevel consistencyLevel, Point point) {
		unBatchedCount.incrementAndGet();
		writeCount.incrementAndGet();
		writeLine(database, retentionPolicy, consistencyLevel, point.lineProtocol());
	}
	
	private void writeLine(String database, String retentionPolicy, ConsistencyLevel consistency, String line) {
		TypedString lineProtocol = new TypedString(line);
		influxDBService.writePoints(
				username,
				password,
				database,
				retentionPolicy,
				TimeUtil.toTimePrecision(TimeUnit.NANOSECONDS),
				consistency.value(),
				lineProtocol);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryResult query(final Query query) {
		QueryResult response = influxDBService
				.query(username, password, query.getDatabase(), query.getCommand());
		return response;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryResult query(final Query query, final TimeUnit timeUnit) {
		QueryResult response = influxDBService
				.query(username, password, query.getDatabase(), TimeUtil.toTimePrecision(timeUnit) , query.getCommand());
		return response;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createDatabase(final String name) {
		Preconditions.checkArgument(!name.contains("-"), "Databasename cant contain -");
		influxDBService.query(username, password, "CREATE DATABASE " + name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteDatabase(final String name) {
		influxDBService.query(username, password, "DROP DATABASE " + name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> describeDatabases() {
		QueryResult result = influxDBService.query(username, password, "SHOW DATABASES");
		// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["mydb"]]}]}]}
		// Series [name=databases, columns=[name], values=[[mydb], [unittest_1433605300968]]]
		List<List<Object>> databaseNames = result.getResults().get(0).getSeries().get(0).getValues();
		List<String> databases = Lists.newArrayList();
		if(databaseNames != null) {
			for (List<Object> database : databaseNames) {
				databases.add(database.get(0).toString());
			}
		}
		return databases;
	}

}

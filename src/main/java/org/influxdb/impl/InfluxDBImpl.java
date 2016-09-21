package org.influxdb.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.influxdb.InfluxDB;
import org.influxdb.dto.*;
import org.influxdb.impl.BatchProcessor.BatchEntry;
import retrofit.RestAdapter;
import retrofit.client.Client;
import retrofit.client.Header;
import retrofit.client.Response;
import retrofit.mime.TypedString;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


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
	
	public InfluxDBImpl(final String url, final String username, final String password, 
			final Client client) {
		super();
		this.username = username;
		this.password = password;
		this.restAdapter = new RestAdapter.Builder()
				.setEndpoint(url)
				.setErrorHandler(new InfluxDBErrorHandler())
				.setClient(client)
				.build();
		this.influxDBService = this.restAdapter.create(InfluxDBService.class);
	}
	

	@Override
	public InfluxDB setLogLevel(final LogLevel logLevel) {
		switch (logLevel) {
		case NONE:
			this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.NONE);
			break;
		case BASIC:
			this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.BASIC);
			break;
		case HEADERS:
			this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.HEADERS);
			break;
		case FULL:
			this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.FULL);
			break;
		default:
			break;
		}
		this.logLevel = logLevel;
		return this;
	}

	@Override
	public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit) {
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
		this.batchProcessor.flush();
		if (this.logLevel != LogLevel.NONE) {
			System.out.println(
					"total writes:" + this.writeCount.get() + " unbatched:" + this.unBatchedCount.get() + "batchPoints:"
							+ this.batchedCount);
		}
	}
	
	@Override
	public boolean isBatchEnabled() {
		return this.batchEnabled.get();
	}

	@Override
	public Pong ping() {
		Stopwatch watch = Stopwatch.createStarted();
		Response response = this.influxDBService.ping();
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
		TypedString lineProtocol = new TypedString(batchPoints.lineProtocol());
		this.influxDBService.writePoints(
				this.username,
				this.password,
				batchPoints.getDatabase(),
				batchPoints.getRetentionPolicy(),
				TimeUtil.toTimePrecision(TimeUnit.NANOSECONDS),
				batchPoints.getConsistency().value(),
				lineProtocol);

	}

	@Override
	public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistency, final String records) {
		this.influxDBService.writePoints(
				this.username,
				this.password,
				database,
				retentionPolicy,
				TimeUtil.toTimePrecision(TimeUnit.NANOSECONDS),
				consistency.value(),
				new TypedString(records));
	}
	@Override
	public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistency, final List<String> records) {
		final String joinedRecords = Joiner.on("\n").join(records);
		write(database, retentionPolicy, consistency, joinedRecords);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryResult query(final Query query) {
		QueryResult response = this.influxDBService
				.query(this.username, this.password, query.getDatabase(), query.getCommand());
		return response;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryResult query(final Query query, final TimeUnit timeUnit) {
		QueryResult response = this.influxDBService
				.query(this.username, this.password, query.getDatabase(), TimeUtil.toTimePrecision(timeUnit) , query.getCommand());
		return response;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createDatabase(final String name) {
		Preconditions.checkArgument(!name.contains("-"), "Database name cant contain -");
		String createDatabaseQueryString = String.format("CREATE DATABASE \"%s\"", name);
		if ( this.version().startsWith("0.") ) {
			createDatabaseQueryString = String.format("CREATE DATABASE IF NOT EXISTS \"%s\"", name);
		}
		this.influxDBService.query(this.username, this.password, createDatabaseQueryString);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteDatabase(final String name) {
		this.influxDBService.query(this.username, this.password, "DROP DATABASE \"" + name + "\"");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> describeDatabases() {
		QueryResult result = this.influxDBService.query(this.username, this.password, "SHOW DATABASES");
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

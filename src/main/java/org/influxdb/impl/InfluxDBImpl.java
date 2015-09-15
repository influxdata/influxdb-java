package org.influxdb.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.squareup.okhttp.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.dto.*;
import org.influxdb.impl.BatchProcessor.BatchEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit.RestAdapter;
import retrofit.RestAdapter.Builder;
import retrofit.client.Header;
import retrofit.client.OkClient;
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
   private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBImpl.class);

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
	public InfluxDBImpl(String url, String username, String password) {
		this.username = username;
		this.password = password;
      restAdapter = new Builder()
				.setEndpoint(url)
				.setErrorHandler(new InfluxDBErrorHandler())
				.setClient(new OkClient(new OkHttpClient()))
				.build();
      influxDBService = restAdapter.create(InfluxDBService.class);
	}

	@Override
	public InfluxDB setLogLevel(LogLevel logLevel) {
		switch (logLevel) {
		case NONE:
         restAdapter.setLogLevel(RestAdapter.LogLevel.NONE);
			break;
		case BASIC:
         restAdapter.setLogLevel(RestAdapter.LogLevel.BASIC);
			break;
		case HEADERS:
         restAdapter.setLogLevel(RestAdapter.LogLevel.HEADERS);
			break;
		case FULL:
         restAdapter.setLogLevel(RestAdapter.LogLevel.FULL);
			break;
		default:
			break;
		}
		this.logLevel = logLevel;
		return this;
	}

	@Override
	public InfluxDB enableBatch(int actions, int flushDuration, TimeUnit flushDurationTimeUnit) {
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
         LOGGER.info("total writes: {}, unbatched: {} batchPoints: {}", writeCount.get(), unBatchedCount.get(), batchedCount);
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
	public void write(String database, String retentionPolicy, Point point) {
		if (batchEnabled.get()) {
			BatchEntry batchEntry = new BatchEntry(point, database, retentionPolicy);
         batchProcessor.put(batchEntry);
		} else {
			BatchPoints batchPoints = BatchPoints.database(database).retentionPolicy(retentionPolicy).build();
			batchPoints.point(point);
         write(batchPoints);
         unBatchedCount.incrementAndGet();
		}
      writeCount.incrementAndGet();
	}

	@Override
	public void write(BatchPoints batchPoints) {
      batchedCount.addAndGet(batchPoints.getPoints().size());
		TypedString lineProtocol = new TypedString(batchPoints.lineProtocol());
      influxDBService.writePoints(
              username,
              password,
				batchPoints.getDatabase(),
				batchPoints.getRetentionPolicy(),
				TimeUtil.toTimePrecision(TimeUnit.NANOSECONDS),
				batchPoints.getConsistency().value(),
				lineProtocol);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryResult query(Query query) {
      return influxDBService
            .query(username, password, query.getDatabase(), query.getCommand());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public QueryResult query(Query query, TimeUnit timeUnit) {
      return influxDBService
            .query(username, password, query.getDatabase(), TimeUtil.toTimePrecision(timeUnit) , query.getCommand());
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createDatabase(String name) {
		Preconditions.checkArgument(!name.contains("-"), "Databasename cant contain -");
      influxDBService.query(username, password, "CREATE DATABASE " + name);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deleteDatabase(String name) {
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

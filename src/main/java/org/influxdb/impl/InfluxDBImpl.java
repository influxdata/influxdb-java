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

import retrofit.RestAdapter;
import retrofit.client.Header;
import retrofit.client.OkClient;
import retrofit.client.Response;
import retrofit.mime.TypedString;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.squareup.okhttp.OkHttpClient;

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
        OkHttpClient okHttpClient = new OkHttpClient();
        this.restAdapter = new RestAdapter.Builder().setEndpoint(url).setErrorHandler(new InfluxDBErrorHandler())
                .setClient(new OkClient(okHttpClient)).build();
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
        this.batchProcessor = BatchProcessor.builder(this).actions(actions)
                .interval(flushDuration, flushDurationTimeUnit).build();
        this.batchEnabled.set(true);
        return this;
    }

    @Override
    public void disableBatch() {
        this.batchEnabled.set(false);
        this.batchProcessor.flush();
        if (this.logLevel != LogLevel.NONE) {
            System.out.println("total writes:" + this.writeCount.get() + " unbatched:" + this.unBatchedCount.get()
                    + "batchPoints:" + this.batchedCount);
        }
    }

    @Override
    public Pong ping() {
        long startTime = System.currentTimeMillis();
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
        pong.setResponseTime(System.currentTimeMillis() - startTime);
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
        }
        else {
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
        this.influxDBService.writePoints(this.username, this.password, batchPoints.getDatabase(), batchPoints
                .getRetentionPolicy(), TimeUtil.toTimePrecision(TimeUnit.NANOSECONDS), batchPoints.getConsistency()
                        .value(), lineProtocol);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QueryResult query(final Query query) {
        QueryResult response = this.influxDBService.query(this.username, this.password, query.getDatabase(),
                                                          query.getCommand());
        return response;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createDatabase(final String name) {
        Preconditions.checkArgument(!name.contains("-"), "Databasename cant contain -");
        this.influxDBService.query(this.username, this.password, "CREATE DATABASE " + name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDatabase(final String name) {
        this.influxDBService.query(this.username, this.password, "DROP DATABASE " + name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> describeDatabases() {
        QueryResult result = this.influxDBService.query(this.username, this.password, "SHOW DATABASES");
        // {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["mydb"]]}]}]}
        // Series [name=databases, columns=[name], values=[[mydb],
        // [unittest_1433605300968]]]
        List<List<Object>> databaseNames = result.getResults().get(0).getSeries().get(0).getValues();
        List<String> databases = Lists.newArrayList();
        if (!databases.isEmpty()) {
            for (List<Object> database : databaseNames) {
                databases.add(database.get(0).toString());
            }
        }
        return databases;
    }

}

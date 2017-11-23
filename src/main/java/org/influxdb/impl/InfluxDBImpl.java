package org.influxdb.impl;


import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.logging.HttpLoggingInterceptor.Level;
import okio.BufferedSource;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.BatchProcessor.HttpBatchEntry;
import org.influxdb.impl.BatchProcessor.UdpBatchEntry;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.moshi.MoshiConverterFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Implementation of a InluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 */
public class InfluxDBImpl implements InfluxDB {

  static final okhttp3.MediaType MEDIA_TYPE_STRING = MediaType.parse("text/plain");

  private static final String SHOW_DATABASE_COMMAND_ENCODED = Query.encode("SHOW DATABASES");

  private final InetAddress hostAddress;
  private final String username;
  private final String password;
  private final Retrofit retrofit;
  private final InfluxDBService influxDBService;
  private BatchProcessor batchProcessor;
  private final AtomicBoolean batchEnabled = new AtomicBoolean(false);
  private final LongAdder writeCount = new LongAdder();
  private final LongAdder unBatchedCount = new LongAdder();
  private final LongAdder batchedCount = new LongAdder();
  private volatile DatagramSocket datagramSocket;
  private final HttpLoggingInterceptor loggingInterceptor;
  private final GzipRequestInterceptor gzipRequestInterceptor;
  private LogLevel logLevel = LogLevel.NONE;
  private JsonAdapter<QueryResult> adapter;
  private String database;
  private String retentionPolicy = "autogen";
  private ConsistencyLevel consistency = ConsistencyLevel.ONE;

  public InfluxDBImpl(final String url, final String username, final String password,
      final OkHttpClient.Builder client) {
    super();
    Moshi moshi = new Moshi.Builder().build();
    this.hostAddress = parseHostAddress(url);
    this.username = username;
    this.password = password;
    this.loggingInterceptor = new HttpLoggingInterceptor();
    this.loggingInterceptor.setLevel(Level.NONE);
    this.gzipRequestInterceptor = new GzipRequestInterceptor();
    this.retrofit = new Retrofit.Builder()
        .baseUrl(url)
        .client(client.addInterceptor(loggingInterceptor).addInterceptor(gzipRequestInterceptor).build())
        .addConverterFactory(MoshiConverterFactory.create())
        .build();
    this.influxDBService = this.retrofit.create(InfluxDBService.class);
    this.adapter = moshi.adapter(QueryResult.class);
  }

    InfluxDBImpl(final String url, final String username, final String password, final OkHttpClient.Builder client,
            final InfluxDBService influxDBService, final JsonAdapter<QueryResult> adapter) {
        super();
        this.hostAddress = parseHostAddress(url);
        this.username = username;
        this.password = password;
        this.loggingInterceptor = new HttpLoggingInterceptor();
        this.loggingInterceptor.setLevel(Level.NONE);
        this.gzipRequestInterceptor = new GzipRequestInterceptor();
        this.retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .client(client.addInterceptor(loggingInterceptor).addInterceptor(gzipRequestInterceptor).build())
                .addConverterFactory(MoshiConverterFactory.create())
                .build();
        this.influxDBService = influxDBService;
        this.adapter = adapter;
    }

  public InfluxDBImpl(final String url, final String username, final String password,
                      final OkHttpClient.Builder client, final String database,
                      final String retentionPolicy, final ConsistencyLevel consistency) {
    this(url, username, password, client);

    setConsistency(consistency);
    setDatabase(database);
    setRetentionPolicy(retentionPolicy);
  }

  private InetAddress parseHostAddress(final String url) {
      HttpUrl httpUrl = HttpUrl.parse(url);

      if (httpUrl == null) {
          throw new IllegalArgumentException("Unable to parse url: " + url);
      }

      try {
          return InetAddress.getByName(httpUrl.host());
      } catch (UnknownHostException e) {
          throw new InfluxDBIOException(e);
      }
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

  /**
   * {@inheritDoc}
   */
  @Override
  public InfluxDB enableGzip() {
    this.gzipRequestInterceptor.enable();
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InfluxDB disableGzip() {
    this.gzipRequestInterceptor.disable();
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isGzipEnabled() {
    return this.gzipRequestInterceptor.isEnabled();
  }

  @Override
  public InfluxDB enableBatch(final int actions, final int flushDuration,
                              final TimeUnit flushDurationTimeUnit) {
    enableBatch(actions, flushDuration, flushDurationTimeUnit, Executors.defaultThreadFactory());
    return this;
  }

  @Override
  public InfluxDB enableBatch(final int actions, final int flushDuration,
                              final TimeUnit flushDurationTimeUnit, final ThreadFactory threadFactory) {
    enableBatch(actions, flushDuration, flushDurationTimeUnit, threadFactory, (points, throwable) -> { });
    return this;
  }

  @Override
  public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit,
                              final ThreadFactory threadFactory,
                              final BiConsumer<Iterable<Point>, Throwable> exceptionHandler,
                              final ConsistencyLevel consistency) {
    enableBatch(actions, flushDuration, flushDurationTimeUnit, threadFactory, exceptionHandler)
        .setConsistency(consistency);
    return this;
  }

  @Override
  public InfluxDB enableBatch(final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit,
                              final ThreadFactory threadFactory,
                              final BiConsumer<Iterable<Point>, Throwable> exceptionHandler) {
    if (this.batchEnabled.get()) {
      throw new IllegalStateException("BatchProcessing is already enabled.");
    }
    this.batchProcessor = BatchProcessor
            .builder(this)
            .actions(actions)
            .exceptionHandler(exceptionHandler)
            .interval(flushDuration, flushDurationTimeUnit)
            .threadFactory(threadFactory)
            .consistencyLevel(consistency)
            .build();
    this.batchEnabled.set(true);
    return this;
  }

  @Override
  public void disableBatch() {
    this.batchEnabled.set(false);
    if (this.batchProcessor != null) {
      this.batchProcessor.flushAndShutdown();
    }
  }

  @Override
  public boolean isBatchEnabled() {
    return this.batchEnabled.get();
  }

  @Override
  public Pong ping() {
    final long started = System.currentTimeMillis();
    Call<ResponseBody> call = this.influxDBService.ping();
    try {
      Response<ResponseBody> response = call.execute();
      Headers headers = response.headers();
      String version = "unknown";
      for (String name : headers.toMultimap().keySet()) {
        if (null != name && "X-Influxdb-Version".equalsIgnoreCase(name)) {
          version = headers.get(name);
          break;
        }
      }
      Pong pong = new Pong();
      pong.setVersion(version);
      pong.setResponseTime(System.currentTimeMillis() - started);
      return pong;
    } catch (IOException e) {
      throw new InfluxDBIOException(e);
    }
  }

  @Override
  public String version() {
    return ping().getVersion();
  }

  @Override
  public void write(final Point point) {
    write(database, retentionPolicy, point);
  }

  @Override
  public void write(final String records) {
    write(database, retentionPolicy, consistency, records);
  }

  @Override
  public void write(final List<String> records) {
    write(database, retentionPolicy, consistency, records);
  }

  @Override
  public void write(final String database, final String retentionPolicy, final Point point) {
    if (this.batchEnabled.get()) {
      HttpBatchEntry batchEntry = new HttpBatchEntry(point, database, retentionPolicy);
      this.batchProcessor.put(batchEntry);
    } else {
      BatchPoints batchPoints = BatchPoints.database(database)
                                           .retentionPolicy(retentionPolicy).build();
      batchPoints.point(point);
      this.write(batchPoints);
      this.unBatchedCount.increment();
    }
    this.writeCount.increment();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(final int udpPort, final Point point) {
    if (this.batchEnabled.get()) {
      UdpBatchEntry batchEntry = new UdpBatchEntry(point, udpPort);
      this.batchProcessor.put(batchEntry);
    } else {
      this.write(udpPort, point.lineProtocol());
      this.unBatchedCount.increment();
    }
    this.writeCount.increment();
  }

  @Override
  public void write(final BatchPoints batchPoints) {
    this.batchedCount.add(batchPoints.getPoints().size());
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
      final String records) {
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
      final List<String> records) {
    write(database, retentionPolicy, consistency, String.join("\n", records));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(final int udpPort, final String records) {
    initialDatagramSocket();
    byte[] bytes = records.getBytes(StandardCharsets.UTF_8);
    try {
        datagramSocket.send(new DatagramPacket(bytes, bytes.length, hostAddress, udpPort));
    } catch (IOException e) {
        throw new InfluxDBIOException(e);
    }
  }

  private void initialDatagramSocket() {
    if (datagramSocket == null) {
        synchronized (InfluxDBImpl.class) {
            if (datagramSocket == null) {
                try {
                    datagramSocket = new DatagramSocket();
                } catch (SocketException e) {
                    throw new InfluxDBIOException(e);
                }
            }
        }
    }
}

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(final int udpPort, final List<String> records) {
    write(udpPort, String.join("\n", records));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public QueryResult query(final Query query) {
    return execute(callQuery(query));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void query(final Query query, final Consumer<QueryResult> onSuccess, final Consumer<Throwable> onFailure) {
    final Call<QueryResult> call = callQuery(query);
    call.enqueue(new Callback<QueryResult>() {
      @Override
      public void onResponse(final Call<QueryResult> call, final Response<QueryResult> response) {
        onSuccess.accept(response.body());
      }

      @Override
      public void onFailure(final Call<QueryResult> call, final Throwable throwable) {
        onFailure.accept(throwable);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
    public void query(final Query query, final int chunkSize, final Consumer<QueryResult> consumer) {

        if (version().startsWith("0.") || version().startsWith("1.0")) {
            throw new UnsupportedOperationException("chunking not supported");
        }

        Call<ResponseBody> call = this.influxDBService.query(this.username, this.password,
                query.getDatabase(), query.getCommandWithUrlEncoded(), chunkSize);

        call.enqueue(new Callback<ResponseBody>() {
            @Override
            public void onResponse(final Call<ResponseBody> call, final Response<ResponseBody> response) {
                try {
                    if (response.isSuccessful()) {
                        BufferedSource source = response.body().source();
                        while (true) {
                            QueryResult result = InfluxDBImpl.this.adapter.fromJson(source);
                            if (result != null) {
                                consumer.accept(result);
                            }
                        }
                    }
                    try (ResponseBody errorBody = response.errorBody()) {
                        throw new InfluxDBException(errorBody.string());
                    }
                } catch (EOFException e) {
                    QueryResult queryResult = new QueryResult();
                    queryResult.setError("DONE");
                    consumer.accept(queryResult);
                } catch (IOException e) {
                    QueryResult queryResult = new QueryResult();
                    queryResult.setError(e.toString());
                    consumer.accept(queryResult);
                }
            }

            @Override
            public void onFailure(final Call<ResponseBody> call, final Throwable t) {
                throw new InfluxDBException(t);
            }
        });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public QueryResult query(final Query query, final TimeUnit timeUnit) {
    return execute(this.influxDBService.query(this.username, this.password, query.getDatabase(),
        TimeUtil.toTimePrecision(timeUnit), query.getCommandWithUrlEncoded()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createDatabase(final String name) {
    Preconditions.checkNonEmptyString(name, "name");
    String createDatabaseQueryString = String.format("CREATE DATABASE \"%s\"", name);
    if (this.version().startsWith("0.")) {
      createDatabaseQueryString = String.format("CREATE DATABASE IF NOT EXISTS \"%s\"", name);
    }
    execute(this.influxDBService.postQuery(this.username, this.password, Query.encode(createDatabaseQueryString)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteDatabase(final String name) {
    execute(this.influxDBService.postQuery(this.username, this.password,
                                           Query.encode("DROP DATABASE \"" + name + "\"")));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> describeDatabases() {
    QueryResult result = execute(this.influxDBService.query(this.username,
                                                            this.password, SHOW_DATABASE_COMMAND_ENCODED));
    // {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["mydb"]]}]}]}
    // Series [name=databases, columns=[name], values=[[mydb], [unittest_1433605300968]]]
    List<List<Object>> databaseNames = result.getResults().get(0).getSeries().get(0).getValues();
    List<String> databases = new ArrayList<>();
    if (databaseNames != null) {
      for (List<Object> database : databaseNames) {
        databases.add(database.get(0).toString());
      }
    }
    return databases;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean databaseExists(final String name) {
    List<String> databases = this.describeDatabases();
    for (String databaseName : databases) {
      if (databaseName.trim().equals(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Calls the influxDBService for the query.
   */
  private Call<QueryResult> callQuery(final Query query) {
    Call<QueryResult> call;
    if (query.requiresPost()) {
      call = this.influxDBService.postQuery(this.username,
              this.password, query.getDatabase(), query.getCommandWithUrlEncoded());
    } else {
      call = this.influxDBService.query(this.username,
              this.password, query.getDatabase(), query.getCommandWithUrlEncoded());
    }
    return call;
  }


  private <T> T execute(final Call<T> call) {
    try {
      Response<T> response = call.execute();
      if (response.isSuccessful()) {
        return response.body();
      }
      try (ResponseBody errorBody = response.errorBody()) {
        throw new InfluxDBException(errorBody.string());
      }
    } catch (IOException e) {
      throw new InfluxDBIOException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    if (!batchEnabled.get()) {
      throw new IllegalStateException("BatchProcessing is not enabled.");
    }
    batchProcessor.flush();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    try {
        this.disableBatch();
    } finally {
        if (datagramSocket != null && !datagramSocket.isClosed()) {
            datagramSocket.close();
        }
    }
  }

  @Override
  public InfluxDB setConsistency(final ConsistencyLevel consistency) {
    this.consistency = consistency;
    return this;
  }

  @Override
  public InfluxDB setDatabase(final String database) {
    this.database = database;
    return this;
  }

  @Override
  public InfluxDB setRetentionPolicy(final String retentionPolicy) {
    this.retentionPolicy = retentionPolicy;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createRetentionPolicy(final String rpName, final String database, final String duration,
                                    final String shardDuration, final int replicationFactor, final boolean isDefault) {
    Preconditions.checkNonEmptyString(rpName, "retentionPolicyName");
    Preconditions.checkNonEmptyString(database, "database");
    Preconditions.checkNonEmptyString(duration, "retentionDuration");
    Preconditions.checkDuration(duration, "retentionDuration");
    if (shardDuration != null && !shardDuration.isEmpty()) {
      Preconditions.checkDuration(shardDuration, "shardDuration");
    }
    Preconditions.checkPositiveNumber(replicationFactor, "replicationFactor");

    StringBuilder queryBuilder = new StringBuilder("CREATE RETENTION POLICY \"");
    queryBuilder.append(rpName)
        .append("\" ON \"")
        .append(database)
        .append("\" DURATION ")
        .append(duration)
        .append(" REPLICATION ")
        .append(replicationFactor);
    if (shardDuration != null && !shardDuration.isEmpty()) {
      queryBuilder.append(" SHARD DURATION ");
      queryBuilder.append(shardDuration);
    }
    if (isDefault) {
      queryBuilder.append(" DEFAULT");
    }
    execute(this.influxDBService.postQuery(this.username, this.password, Query.encode(queryBuilder.toString())));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createRetentionPolicy(final String rpName, final String database, final String duration,
                                    final int replicationFactor, final boolean isDefault) {
    createRetentionPolicy(rpName, database, duration, null, replicationFactor, isDefault);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createRetentionPolicy(final String rpName, final String database, final String duration,
                                    final String shardDuration, final int replicationFactor) {
    createRetentionPolicy(rpName, database, duration, null, replicationFactor, false);
  }

  /**
   * {@inheritDoc}
   * @param rpName the name of the retentionPolicy
   * @param database the name of the database
   */
  @Override
  public void dropRetentionPolicy(final String rpName, final String database) {
    Preconditions.checkNonEmptyString(rpName, "retentionPolicyName");
    Preconditions.checkNonEmptyString(database, "database");
    StringBuilder queryBuilder = new StringBuilder("DROP RETENTION POLICY \"");
    queryBuilder.append(rpName)
        .append("\" ON \"")
        .append(database)
        .append("\"");
    execute(this.influxDBService.postQuery(this.username, this.password,
        Query.encode(queryBuilder.toString())));
  }
}

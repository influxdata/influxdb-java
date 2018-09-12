package org.influxdb.impl;


import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.logging.HttpLoggingInterceptor.Level;
import okio.BufferedSource;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.BoundParameterQuery;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.BatchProcessor.HttpBatchEntry;
import org.influxdb.impl.BatchProcessor.UdpBatchEntry;
import org.influxdb.msgpack.MessagePackConverterFactory;
import org.influxdb.msgpack.MessagePackTraverser;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Converter.Factory;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.moshi.MoshiConverterFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of a InluxDB API.
 *
 * @author stefan.majer [at] gmail.com
 */
public class InfluxDBImpl implements InfluxDB {

  private static final String APPLICATION_MSGPACK = "application/x-msgpack";

  static final okhttp3.MediaType MEDIA_TYPE_STRING = MediaType.parse("text/plain");

  private static final String SHOW_DATABASE_COMMAND_ENCODED = Query.encode("SHOW DATABASES");

  /**
   * This static constant holds the http logging log level expected in DEBUG mode
   * It is set by System property {@code org.influxdb.InfluxDB.logLevel}.
   *
   * @see org.influxdb.InfluxDB#LOG_LEVEL_PROPERTY
   */
  private static final LogLevel LOG_LEVEL = LogLevel.parseLogLevel(System.getProperty(LOG_LEVEL_PROPERTY));

  private final String hostName;
  private String version;
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
  private String database;
  private String retentionPolicy = "autogen";
  private ConsistencyLevel consistency = ConsistencyLevel.ONE;
  private final boolean messagePack;
  private Boolean messagePackSupport;
  private final ChunkProccesor chunkProccesor;

  /**
   * Constructs a new {@code InfluxDBImpl}.
   *
   * @param url
   *          The InfluxDB server API URL
   * @param username
   *          The InfluxDB user name
   * @param password
   *          The InfluxDB user password
   * @param okHttpBuilder
   *          The OkHttp Client Builder
   * @param responseFormat
   *          The {@code ResponseFormat} to use for response from InfluxDB
   *          server
   */
  public InfluxDBImpl(final String url, final String username, final String password,
                      final OkHttpClient.Builder okHttpBuilder, final ResponseFormat responseFormat) {
    this(url, username, password, okHttpBuilder, new Retrofit.Builder(), responseFormat);
  }

  /**
   * Constructs a new {@code InfluxDBImpl}.
   *
   * @param url
   *          The InfluxDB server API URL
   * @param username
   *          The InfluxDB user name
   * @param password
   *          The InfluxDB user password
   * @param okHttpBuilder
   *          The OkHttp Client Builder
   * @param retrofitBuilder
   *          The Retrofit Builder
   * @param responseFormat
   *          The {@code ResponseFormat} to use for response from InfluxDB
   *          server
   */
  public InfluxDBImpl(final String url, final String username, final String password,
                      final OkHttpClient.Builder okHttpBuilder, final Retrofit.Builder retrofitBuilder,
                      final ResponseFormat responseFormat) {
    this.messagePack = ResponseFormat.MSGPACK.equals(responseFormat);
    this.hostName = parseHost(url);

    this.loggingInterceptor = new HttpLoggingInterceptor();
    setLogLevel(LOG_LEVEL);

    this.gzipRequestInterceptor = new GzipRequestInterceptor();
    OkHttpClient.Builder clonedOkHttpBuilder = okHttpBuilder.build().newBuilder();
    clonedOkHttpBuilder.addInterceptor(loggingInterceptor).addInterceptor(gzipRequestInterceptor).
      addInterceptor(new BasicAuthInterceptor(username, password));
    Factory converterFactory = null;
    switch (responseFormat) {
    case MSGPACK:
      clonedOkHttpBuilder.addInterceptor(chain -> {
        Request request = chain.request().newBuilder().addHeader("Accept", APPLICATION_MSGPACK).build();
        return chain.proceed(request);
      });

      converterFactory = MessagePackConverterFactory.create();
      chunkProccesor = new MessagePackChunkProccesor();
      break;
    case JSON:
    default:
      converterFactory = MoshiConverterFactory.create();

      Moshi moshi = new Moshi.Builder().build();
      JsonAdapter<QueryResult> adapter = moshi.adapter(QueryResult.class);
      chunkProccesor = new JSONChunkProccesor(adapter);
      break;
    }

    Retrofit.Builder clonedRetrofitBuilder = retrofitBuilder.baseUrl(url).build().newBuilder();
    this.retrofit = clonedRetrofitBuilder.client(clonedOkHttpBuilder.build())
            .addConverterFactory(converterFactory).build();
    this.influxDBService = this.retrofit.create(InfluxDBService.class);

  }

  public InfluxDBImpl(final String url, final String username, final String password,
      final OkHttpClient.Builder client) {
    this(url, username, password, client, ResponseFormat.JSON);

  }

  InfluxDBImpl(final String url, final String username, final String password, final OkHttpClient.Builder client,
      final InfluxDBService influxDBService, final JsonAdapter<QueryResult> adapter) {
    super();
    this.messagePack = false;
    this.hostName = parseHost(url);

    this.loggingInterceptor = new HttpLoggingInterceptor();
    setLogLevel(LOG_LEVEL);

    this.gzipRequestInterceptor = new GzipRequestInterceptor();
    OkHttpClient.Builder clonedBuilder = client.build().newBuilder();
    this.retrofit = new Retrofit.Builder().baseUrl(url)
        .client(clonedBuilder.addInterceptor(loggingInterceptor).addInterceptor(gzipRequestInterceptor).
            addInterceptor(new BasicAuthInterceptor(username, password)).build())
        .addConverterFactory(MoshiConverterFactory.create()).build();
    this.influxDBService = influxDBService;

    chunkProccesor = new JSONChunkProccesor(adapter);
  }

  public InfluxDBImpl(final String url, final String username, final String password, final OkHttpClient.Builder client,
      final String database, final String retentionPolicy, final ConsistencyLevel consistency) {
    this(url, username, password, client);

    setConsistency(consistency);
    setDatabase(database);
    setRetentionPolicy(retentionPolicy);
  }

  private String parseHost(final String url) {
    String hostName;
    try {
      URI uri = new URI(url);
      hostName = uri.getHost();
    } catch (URISyntaxException e1) {
      throw new IllegalArgumentException("Unable to parse url: " + url, e1);
    }

    if (hostName == null) {
      throw new IllegalArgumentException("Unable to parse url: " + url);
    }

    try {
      InetAddress.getByName(hostName);
    } catch (UnknownHostException e) {
      throw new InfluxDBIOException(e);
    }
    return hostName;
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
  public InfluxDB enableBatch() {
    enableBatch(BatchOptions.DEFAULTS);
    return this;
  }

  @Override
  public InfluxDB enableBatch(final BatchOptions batchOptions) {

    if (this.batchEnabled.get()) {
      throw new IllegalStateException("BatchProcessing is already enabled.");
    }
    this.batchProcessor = BatchProcessor
            .builder(this)
            .actions(batchOptions.getActions())
            .exceptionHandler(batchOptions.getExceptionHandler())
            .interval(batchOptions.getFlushDuration(), batchOptions.getJitterDuration(), TimeUnit.MILLISECONDS)
            .threadFactory(batchOptions.getThreadFactory())
            .bufferLimit(batchOptions.getBufferLimit())
            .consistencyLevel(batchOptions.getConsistency())
            .build();
    this.batchEnabled.set(true);
    return this;
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
    enableBatch(actions, flushDuration, 0, flushDurationTimeUnit, threadFactory, exceptionHandler);
    return this;
  }

  private InfluxDB enableBatch(final int actions, final int flushDuration, final int jitterDuration,
                               final TimeUnit durationTimeUnit, final ThreadFactory threadFactory,
                               final BiConsumer<Iterable<Point>, Throwable> exceptionHandler) {
    if (this.batchEnabled.get()) {
      throw new IllegalStateException("BatchProcessing is already enabled.");
    }
    this.batchProcessor = BatchProcessor
            .builder(this)
            .actions(actions)
            .exceptionHandler(exceptionHandler)
            .interval(flushDuration, jitterDuration, durationTimeUnit)
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
    if (version == null) {
      this.version = ping().getVersion();
    }
    return this.version;
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
        batchPoints.getDatabase(),
        batchPoints.getRetentionPolicy(),
        TimeUtil.toTimePrecision(batchPoints.getPrecision()),
        batchPoints.getConsistency().value(),
        lineProtocol));
  }

  @Override
  public void writeWithRetry(final BatchPoints batchPoints) {
    if (isBatchEnabled()) {
      batchProcessor.getBatchWriter().write(Collections.singleton(batchPoints));
    } else {
      write(batchPoints);
    }
  }

  @Override
  public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
          final TimeUnit precision, final String records) {
    execute(this.influxDBService.writePoints(
        database,
        retentionPolicy,
        TimeUtil.toTimePrecision(precision),
        consistency.value(),
        RequestBody.create(MEDIA_TYPE_STRING, records)));
  }

  @Override
  public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
      final String records) {
    write(database, retentionPolicy, consistency, TimeUnit.NANOSECONDS, records);
  }

  @Override
  public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
      final List<String> records) {
    write(database, retentionPolicy, consistency, TimeUnit.NANOSECONDS, records);
  }


  @Override
  public void write(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
          final TimeUnit precision, final List<String> records) {
    write(database, retentionPolicy, consistency, precision, String.join("\n", records));
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void write(final int udpPort, final String records) {
    initialDatagramSocket();
    byte[] bytes = records.getBytes(StandardCharsets.UTF_8);
    try {
        datagramSocket.send(new DatagramPacket(bytes, bytes.length, new InetSocketAddress(hostName, udpPort)));
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
    return executeQuery(callQuery(query));
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
  public void query(final Query query, final int chunkSize, final Consumer<QueryResult> onNext) {
    query(query, chunkSize, onNext, () -> { });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void query(final Query query, final int chunkSize, final BiConsumer<Cancellable, QueryResult> onNext) {
    query(query, chunkSize, onNext, () -> { });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void query(final Query query, final int chunkSize, final Consumer<QueryResult> onNext,
                    final Runnable onComplete) {
    query(query, chunkSize, (cancellable, queryResult) -> onNext.accept(queryResult), onComplete);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void query(final Query query, final int chunkSize, final BiConsumer<Cancellable, QueryResult> onNext,
                    final Runnable onComplete) {
        Call<ResponseBody> call = null;
        if (query instanceof BoundParameterQuery) {
            BoundParameterQuery boundParameterQuery = (BoundParameterQuery) query;
            call = this.influxDBService.query(query.getDatabase(), query.getCommandWithUrlEncoded(), chunkSize,
                    boundParameterQuery.getParameterJsonWithUrlEncoded());
        } else {
            call = this.influxDBService.query(query.getDatabase(), query.getCommandWithUrlEncoded(), chunkSize);
        }

    call.enqueue(new Callback<ResponseBody>() {
      @Override
      public void onResponse(final Call<ResponseBody> call, final Response<ResponseBody> response) {

        Cancellable cancellable = new Cancellable() {
          @Override
          public void cancel() {
            call.cancel();
          }

          @Override
          public boolean isCanceled() {
            return call.isCanceled();
          }
        };

        try {
          if (response.isSuccessful()) {
            ResponseBody chunkedBody = response.body();
            chunkProccesor.process(chunkedBody, cancellable, onNext, onComplete);
          } else {
            // REVIEW: must be handled consistently with IOException.
            ResponseBody errorBody = response.errorBody();
            if (errorBody != null) {
              throw new InfluxDBException(errorBody.string());
            }
          }
        } catch (IOException e) {
          QueryResult queryResult = new QueryResult();
          queryResult.setError(e.toString());
          onNext.accept(cancellable, queryResult);
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
    Call<QueryResult> call = null;
    if (query instanceof BoundParameterQuery) {
        BoundParameterQuery boundParameterQuery = (BoundParameterQuery) query;
        call = this.influxDBService.query(query.getDatabase(),
                TimeUtil.toTimePrecision(timeUnit), query.getCommandWithUrlEncoded(),
                boundParameterQuery.getParameterJsonWithUrlEncoded());
    } else {
        call = this.influxDBService.query(query.getDatabase(),
                TimeUtil.toTimePrecision(timeUnit), query.getCommandWithUrlEncoded());
    }
    return executeQuery(call);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createDatabase(final String name) {
    Preconditions.checkNonEmptyString(name, "name");
    String createDatabaseQueryString = String.format("CREATE DATABASE \"%s\"", name);
    executeQuery(this.influxDBService.postQuery(Query.encode(createDatabaseQueryString)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteDatabase(final String name) {
    executeQuery(this.influxDBService.postQuery(Query.encode("DROP DATABASE \"" + name + "\"")));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> describeDatabases() {
    QueryResult result = executeQuery(this.influxDBService.query(SHOW_DATABASE_COMMAND_ENCODED));
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
    if (query instanceof BoundParameterQuery) {
        BoundParameterQuery boundParameterQuery = (BoundParameterQuery) query;
        call = this.influxDBService.postQuery(query.getDatabase(), query.getCommandWithUrlEncoded(),
                boundParameterQuery.getParameterJsonWithUrlEncoded());
    } else {
        if (query.requiresPost()) {
          call = this.influxDBService.postQuery(query.getDatabase(), query.getCommandWithUrlEncoded());
        } else {
          call = this.influxDBService.query(query.getDatabase(), query.getCommandWithUrlEncoded());
        }
    }
    return call;
  }

  static class ErrorMessage {
    public String error;
  }

  private boolean checkMessagePackSupport() {
    Matcher matcher = Pattern.compile("(\\d+\\.*)+").matcher(version());
    if (!matcher.find()) {
      return false;
    }
    String s = matcher.group();
    String[] versionNumbers = s.split("\\.");
    final int major = Integer.parseInt(versionNumbers[0]);
    final int minor = Integer.parseInt(versionNumbers[1]);
    final int fromMinor = 4;
    return (major >= 2) || ((major == 1) && (minor >= fromMinor));
  }

  private QueryResult executeQuery(final Call<QueryResult> call) {
    if (messagePack) {
      if (messagePackSupport == null) {
        messagePackSupport = checkMessagePackSupport();
      }

      if (!messagePackSupport) {
        throw new UnsupportedOperationException(
            "MessagePack format is only supported from InfluxDB version 1.4 and later");
      }
    }
    return execute(call);
  }

  private <T> T execute(final Call<T> call) {
    try {
      Response<T> response = call.execute();
      if (response.isSuccessful()) {
        return response.body();
      }
      try (ResponseBody errorBody = response.errorBody()) {
        if (messagePack) {
          throw InfluxDBException.buildExceptionForErrorState(errorBody.byteStream());
        } else {
          throw InfluxDBException.buildExceptionForErrorState(errorBody.string());
        }
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
    executeQuery(this.influxDBService.postQuery(Query.encode(queryBuilder.toString())));
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
    executeQuery(this.influxDBService.postQuery(Query.encode(queryBuilder.toString())));
  }

  private interface ChunkProccesor {
    void process(ResponseBody chunkedBody, Cancellable cancellable,
                 BiConsumer<Cancellable, QueryResult> consumer, Runnable onComplete) throws IOException;
  }

  private class MessagePackChunkProccesor implements ChunkProccesor {
    @Override
    public void process(final ResponseBody chunkedBody, final Cancellable cancellable,
                        final BiConsumer<Cancellable, QueryResult> consumer, final Runnable onComplete)
            throws IOException {
      MessagePackTraverser traverser = new MessagePackTraverser();
      try (InputStream is = chunkedBody.byteStream()) {
        for (Iterator<QueryResult> it = traverser.traverse(is).iterator(); it.hasNext() && !cancellable.isCanceled();) {
          QueryResult result = it.next();
          consumer.accept(cancellable, result);
        }
      }
      if (!cancellable.isCanceled()) {
        onComplete.run();
      }
    }
  }

  private class JSONChunkProccesor implements ChunkProccesor {
    private JsonAdapter<QueryResult> adapter;

    public JSONChunkProccesor(final JsonAdapter<QueryResult> adapter) {
      this.adapter = adapter;
    }

    @Override
    public void process(final ResponseBody chunkedBody, final Cancellable cancellable,
                        final BiConsumer<Cancellable, QueryResult> consumer, final Runnable onComplete)
            throws IOException {
      try {
        BufferedSource source = chunkedBody.source();
        while (!cancellable.isCanceled()) {
          QueryResult result = adapter.fromJson(source);
          if (result != null) {
            consumer.accept(cancellable, result);
          }
        }
      } catch (EOFException e) {
        QueryResult queryResult = new QueryResult();
        queryResult.setError("DONE");
        consumer.accept(cancellable, queryResult);
        if (!cancellable.isCanceled()) {
          onComplete.run();
        }
      } finally {
        chunkedBody.close();
      }
    }
  }
}

package org.influxdb.impl;


import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.exception.DeleteInfluxException;
import org.influxdb.impl.BatchProcessor.HttpBatchEntry;
import org.influxdb.impl.BatchProcessor.UdpBatchEntry;
import retrofit2.Call;
import retrofit2.Callback;
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

  private static final String SHOW_DATABASE_COMMAND_ENCODED = Query.encode("SHOW DATABASES");

  private final InetAddress hostAddress;
  private final String username;
  private final String password;
  private final Retrofit retrofit;
  private final InfluxDBService influxDBService;
  private BatchProcessor batchProcessor;
  private final AtomicBoolean batchEnabled = new AtomicBoolean(false);
  private final AtomicLong writeCount = new AtomicLong();
  private final AtomicLong unBatchedCount = new AtomicLong();
  private final AtomicLong batchedCount = new AtomicLong();
  private volatile DatagramSocket datagramSocket;
  private final HttpLoggingInterceptor loggingInterceptor;
  private final GzipRequestInterceptor gzipRequestInterceptor;
  private LogLevel logLevel = LogLevel.NONE;
  private JsonAdapter<QueryResult> adapter;

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

  private InetAddress parseHostAddress(final String url) {
      try {
          return InetAddress.getByName(HttpUrl.parse(url).host());
      } catch (UnknownHostException e) {
          throw new RuntimeException(e);
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
    if (this.batchEnabled.get()) {
      throw new IllegalStateException("BatchProcessing is already enabled.");
    }
    this.batchProcessor = BatchProcessor
        .builder(this)
        .actions(actions)
        .interval(flushDuration, flushDurationTimeUnit)
        .threadFactory(threadFactory)
        .build();
    this.batchEnabled.set(true);
    return this;
  }

  @Override
  public void disableBatch() {
    this.batchEnabled.set(false);
    if (this.batchProcessor != null) {
      this.batchProcessor.flushAndShutdown();
      if (this.logLevel != LogLevel.NONE) {
        System.out.println(
            "total writes:" + this.writeCount.get()
            + " unbatched:" + this.unBatchedCount.get()
            + " batchPoints:" + this.batchedCount);
      }
    }
  }

  @Override
  public boolean isBatchEnabled() {
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
        if (null != name && "X-Influxdb-Version".equalsIgnoreCase(name)) {
          version = headers.get(name);
          break;
        }
      }
      Pong pong = new Pong();
      pong.setVersion(version);
      pong.setResponseTime(watch.elapsed(TimeUnit.MILLISECONDS));
      return pong;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String version() {
    return ping().getVersion();
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
      this.unBatchedCount.incrementAndGet();
    }
    this.writeCount.incrementAndGet();
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
      this.unBatchedCount.incrementAndGet();
    }
    this.writeCount.incrementAndGet();
  }

  @Override
  public void dropMeasurement(final String database, final String measurement) throws DeleteInfluxException {
    final String query = "DROP MEASUREMENT \"" + measurement + "\"";

    // todo for debug logger
    System.out.println("query: " + query);

    final String encodeQuery = Query.encode(query);
    final QueryResult execute = execute(this.influxDBService.postQuery(this.username, this.password, database,
            encodeQuery));
    final QueryResult.Result result = execute.getResults().get(0);
    if (result.hasError()) {
      throw new DeleteInfluxException(result.getError());
    }
  }

  @Override
  public void delete(final String database, final Point point) throws DeleteInfluxException {
    String query = "DELETE FROM \"" + point.getMeasurement() + "\"";

    if (!point.getFields().isEmpty()) {
      query += " WHERE 1 = 1";
      for (Map.Entry<String, String> entry : point.getTags().entrySet()) {
        query += " AND \"" + entry.getKey() + "\" = \'" + entry.getValue() + "\'";
      }
      final String encodeQuery = Query.encode(query);
      final QueryResult execute = execute(this.influxDBService.postQuery(this.username, this.password, database,
              encodeQuery));
      final QueryResult.Result result = execute.getResults().get(0);
      if (result.hasError()) {
        throw new DeleteInfluxException(result.getError());
      }

      // TODO https://github.com/influxdata/influxdb/issues/3210
//      org.influxdb.exception.DeleteInfluxException: fields not supported in WHERE clause during deletion
//      final Set<Map.Entry<String, Object>> entries = tags.entrySet();
//      for (Map.Entry<String, Object> entry : entries) {
//          query += " AND \"" + entry.getKey() + "\" = \'" + entry.getValue() + "\'";
//      }
    }
  }

  @Override
  public void deleteOld(final String database, final String measurement) throws DeleteInfluxException {
    String query = "DELETE FROM \"" + measurement + "\" WHERE time < now()";

    // todo for debug logger
    System.out.println("query: " + query);

    final String encodeQuery = Query.encode(query);
    final Call<QueryResult> call = this.influxDBService.postQuery(this.username, this.password, database, encodeQuery);
    final QueryResult execute = execute(call);
    final QueryResult.Result result = execute.getResults().get(0);
    if (result.hasError()) {
      throw new DeleteInfluxException(result.getError());
    }
  }

  @Override
  public void deleteBeforeDate(final String database, final String measurement, final Date date)
          throws DeleteInfluxException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    final String timestamps = dateFormat.format(date);
    String query = "DELETE FROM \"" + measurement + "\" WHERE time < '" + timestamps + "'";

    // todo for debug logger
    System.out.println("query: " + query);

    final String encodeQuery = Query.encode(query);
    final Call<QueryResult> call = this.influxDBService.postQuery(this.username, this.password, database, encodeQuery);
    final QueryResult execute = execute(call);
    final QueryResult.Result result = execute.getResults().get(0);
    if (result.hasError()) {
      throw new DeleteInfluxException(result.getError());
    }
  }

  @Override
  public void deleteAfterDate(final String database, final String measurement, final Date date)
          throws DeleteInfluxException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    final String timestamps = dateFormat.format(date);
    String query = "DELETE FROM \"" + measurement + "\" WHERE time > '" + timestamps + "'";

    // todo for debug logger
    System.out.println("query: " + query);

    final String encodeQuery = Query.encode(query);
    final Call<QueryResult> call = this.influxDBService.postQuery(this.username, this.password, database, encodeQuery);
    final QueryResult execute = execute(call);
    final QueryResult.Result result = execute.getResults().get(0);
    if (result.hasError()) {
      throw new DeleteInfluxException(result.getError());
    }
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
    final String joinedRecords = Joiner.on("\n").join(records);
    write(database, retentionPolicy, consistency, joinedRecords);
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
        throw new RuntimeException(e);
    }
  }

  private void initialDatagramSocket() {
    if (datagramSocket == null) {
        synchronized (InfluxDBImpl.class) {
            if (datagramSocket == null) {
                try {
                    datagramSocket = new DatagramSocket();
                } catch (SocketException e) {
                    throw new RuntimeException(e);
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
    final String joinedRecords = Joiner.on("\n").join(records);
    write(udpPort, joinedRecords);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public QueryResult query(final Query query) {
    Call<QueryResult> call;
    if (query.requiresPost()) {
      call = this.influxDBService.postQuery(this.username,
                                            this.password, query.getDatabase(), query.getCommandWithUrlEncoded());
    } else {
      call = this.influxDBService.query(this.username,
                                        this.password, query.getDatabase(), query.getCommandWithUrlEncoded());
    }
    return execute(call);
  }

  /**
   * {@inheritDoc}
   */
  @Override
    public void query(final Query query, final int chunkSize, final Consumer<QueryResult> consumer) {

        if (version().startsWith("0.") || version().startsWith("1.0")) {
            throw new RuntimeException("chunking not supported");
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
                        throw new RuntimeException(errorBody.string());
                    }
                } catch (EOFException e) {
                    QueryResult queryResult = new QueryResult();
                    queryResult.setError("DONE");
                    consumer.accept(queryResult);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onFailure(final Call<ResponseBody> call, final Throwable t) {
                throw new RuntimeException(t);
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
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Database name may not be null or empty");
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
    List<String> databases = Lists.newArrayList();
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

  private <T> T execute(final Call<T> call) {
    try {
      Response<T> response = call.execute();
      if (response.isSuccessful()) {
        // todo when empty don't return list with one empty element
        return response.body();
      }
      try (ResponseBody errorBody = response.errorBody()) {
        throw new RuntimeException(errorBody.string());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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

}

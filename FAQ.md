# Frequently Asked Questions

## Functionality

- [Is the batch part of the client thread safe ?](#is-the-batch-part-of-the-client-thread-safe-)
- [If multiple threads are accessing it, are they all adding Points to the same batch ?](#if-multiple-threads-are-accessing-it-are-they-all-adding-points-to-the-same-batch-)
- [And if so, is there a single thread in the background that is emptying batch to the server ?](#and-if-so-is-there-a-single-thread-in-the-background-that-is-emptying-batch-to-the-server-)
- [If there is an error during this background process, is it propagated to the rest of the client ?](#if-there-is-an-error-during-this-background-process-is-it-propagated-to-the-rest-of-the-client-)
- [How the client responds to concurrent write backpressure from server ?](#how-the-client-responds-to-concurrent-write-backpressure-from-server-)


## Security

- [Is default config security setup TLS 1.2 ?](#is-default-config-security-setup-tls-12-)

## Is the batch part of the client thread safe ?

Yes, the __BatchProcessor__ uses a __BlockingQueue__ and the __RetryCapableBatchWriter__ is synchronized on its __write__ method

```
org.influxdb.impl.RetryCapableBatchWriter.write(Collection<BatchPoints>)

```

## If multiple threads are accessing it, are they all adding Points to the same batch ?

If they share the same InfluxDbImpl instance, so the answer is Yes (all writing points are put to the __BlockingQueue__)

## And if so, is there a single thread in the background that is emptying batch to the server ?

Yes, there is one worker thread that is scheduled to periodically flush the __BlockingQueue__

## If there is an error during this background process, is it propagated to the rest of the client ?

Yes, on initializing BatchOptions, you can pass an exceptionHandler, this handler is used to handle any batch writing that causes a non-recoverable exception or when a batch is evicted due to a retry buffer capacity
(please refer to __BatchOptions.bufferLimit(int)__ for more details)
(list of non-recoverable error : [Handling-errors-of-InfluxDB-under-high-load](https://github.com/influxdata/influxdb-java/wiki/Handling-errors-of-InfluxDB-under-high-load))

## How the client responds to concurrent write backpressure from server ?
Concurrent WRITE throttling at server side is controlled by the trio (__max-concurrent-write-limit__, __max-enqueued-write-limit__, __enqueued-write-timeout__)
for example, you can have these in influxdb.conf
```
max-concurrent-write-limit = 2
max-enqueued-write-limit = 1
enqueued-write-timeout = 1000

```

(more info at this [PR #9888 HTTP Write Throttle](https://github.com/influxdata/influxdb/pull/9888/files))

If the number of concurrent writes reach the threshold, then any further write will be immidiately returned with

```
org.influxdb.InfluxDBIOException: java.net.SocketException: Connection reset by peer: socket write error
               at org.influxdb.impl.InfluxDBImpl.execute(InfluxDBImpl.java:692)
               at org.influxdb.impl.InfluxDBImpl.write(InfluxDBImpl.java:428)

```

Form version 2.9, influxdb-java introduces new error handling feature, the client will try to back off and rewrite failed wites on some recoverable errors (list of recoverable error : [Handling-errors-of-InfluxDB-under-high-load](https://github.com/influxdata/influxdb-java/wiki/Handling-errors-of-InfluxDB-under-high-load))

So in case the number of write requests exceeds Concurrent write setting at server side, influxdb-java can try to make sure no writing points get lost (due to rejection from server)

## Is default config security setup TLS 1.2 ?

(answer need to be verified)

To construct an InfluxDBImpl you will need to pass a OkHttpClient.Builder instance.
At this point you are able to set your custom SSLSocketFactory via method OkHttpClient.Builder.sslSocketFactory(…)

In case you don’t set it, OkHttp will use the system default (Java platform dependent), I tested in Java 8 (influxdb-java has CI test in Java 8 and 10) and see the default SSLContext support these protocols
SSLv3/TLSv1/TLSv1.1/TLSv1.2

So if the server supports TLS1.2, the communication should be encrypted by TLS 1.2 (during the handshake the client will provide the list of accepted security protocols and the server will pick one, so this case the server would pick TLS 1.2)


# Frequently Asked Questions

## Functionality

- [Frequently Asked Questions](#frequently-asked-questions)
  - [Functionality](#functionality)
  - [Security](#security)
  - [Is the batch part of the client thread safe](#is-the-batch-part-of-the-client-thread-safe)
  - [If multiple threads are accessing it, are they all adding Points to the same batch ?](#if-multiple-threads-are-accessing-it-are-they-all-adding-points-to-the-same-batch)
  - [And if so, is there a single thread in the background that is emptying batch to the server ?](#and-if-so-is-there-a-single-thread-in-the-background-that-is-emptying-batch-to-the-server)
  - [If there is an error during this background process, is it propagated to the rest of the client ?](#if-there-is-an-error-during-this-background-process-is-it-propagated-to-the-rest-of-the-client)
  - [How the client responds to concurrent write backpressure from server ?](#how-the-client-responds-to-concurrent-write-backpressure-from-server)
  - [Is there a way to tell that all query chunks have arrived ?](#is-there-a-way-to-tell-that-all-query-chunks-have-arrived)
  - [How to handle exceptions while using async chunked queries ?](#how-to-handle-exceptions-while-using-async-chunked-queries)
  - [Is there a way to tell the system to stop sending more chunks once I've found what I'm looking for ?](#is-there-a-way-to-tell-the-system-to-stop-sending-more-chunks-once-ive-found-what-im-looking-for)
  - [Is default config security setup TLS 1.2 ?](#is-default-config-security-setup-tls-12)
  - [How to use SSL client certificate authentication](#how-to-use-ssl-client-certificate-authentication)

## Security

- [Is default config security setup TLS 1.2 ?](#is-default-config-security-setup-tls-12-)
- [How to use SSL client certificate authentication](#how-to-use-ssl-client-certificate-authentication-)

## Is the batch part of the client thread safe

Yes, the __BatchProcessor__ uses a __BlockingQueue__ and the __RetryCapableBatchWriter__ is synchronized on its __write__ method

```java
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

```properties
max-concurrent-write-limit = 2
max-enqueued-write-limit = 1
enqueued-write-timeout = 1000

```

(more info at this [PR #9888 HTTP Write Throttle](https://github.com/influxdata/influxdb/pull/9888/files))

If the number of concurrent writes reach the threshold, then any further write will be immidiately returned with

```bash
org.influxdb.InfluxDBIOException: java.net.SocketException: Connection reset by peer: socket write error
               at org.influxdb.impl.InfluxDBImpl.execute(InfluxDBImpl.java:692)
               at org.influxdb.impl.InfluxDBImpl.write(InfluxDBImpl.java:428)

```

Form version 2.9, influxdb-java introduces new error handling feature, the client will try to back off and rewrite failed wites on some recoverable errors (list of recoverable error : [Handling-errors-of-InfluxDB-under-high-load](https://github.com/influxdata/influxdb-java/wiki/Handling-errors-of-InfluxDB-under-high-load))

So in case the number of write requests exceeds Concurrent write setting at server side, influxdb-java can try to make sure no writing points get lost (due to rejection from server)

## Is there a way to tell that all query chunks have arrived ?

Yes, there is __onComplete__ action that is invoked after successfully end of stream.

```java
influxDB.query(new Query("SELECT * FROM disk", "telegraf"), 10_000,
    queryResult -> {
        System.out.println("result = " + queryResult);
    },
    () -> {
        System.out.println("The query successfully finished.");
    });
```

## How to handle exceptions while using async chunked queries ?

Exception handling for chunked queries can be handled by __onFailure__ error
consumer.

```java

influxDB.query(query, chunksize,
        //onNext result consumer
        (cancellable, queryResult) -> {
            System.out.println("Process queryResult - " + queryResult.toString());
        }
        //onComplete executable
        , () -> {
            System.out.println("On Complete - the query finished successfully.");
        },
        //onFailure error handler
        throwable -> {
            System.out.println("On Failure - " + throwable.getLocalizedMessage());
        });
```

## Is there a way to tell the system to stop sending more chunks once I've found what I'm looking for ?

Yes, there is __onNext__ bi-consumer with capability to discontinue a streaming query.

```java
influxDB.query(new Query("SELECT * FROM disk", "telegraf"), 10_000, (cancellable, queryResult) -> {

    // found what I'm looking for ?
    if (foundRequest(queryResult)) {
        // yes => cancel query
        cancellable.cancel();
    }

    // no => process next result
    processResult(queryResult);
});
```

## Is default config security setup TLS 1.2 ?

(answer need to be verified)

To construct an InfluxDBImpl you will need to pass a OkHttpClient.Builder instance.
At this point you are able to set your custom SSLSocketFactory via method OkHttpClient.Builder.sslSocketFactory(…)

In case you don’t set it, OkHttp will use the system default (Java platform dependent), I tested in Java 8 (influxdb-java has CI test in Java 8 and 10) and see the default SSLContext support these protocols
SSLv3/TLSv1/TLSv1.1/TLSv1.2

So if the server supports TLS1.2, the communication should be encrypted by TLS 1.2 (during the handshake the client will provide the list of accepted security protocols and the server will pick one, so this case the server would pick TLS 1.2)

## How to use SSL client certificate authentication

To use SSL certificate authentication you need to setup `SslSocketFactory` on OkHttpClient.Builder.

Here is the example, how to create InfluxDB client with the new SSLContext with custom identity keystore (p12) and truststore (jks):

```java
KeyStore keyStore = KeyStore.getInstance("PKCS12");
keyStore.load(new FileInputStream("conf/keystore.p12"), "changeme".toCharArray());

KeyStore trustStore = KeyStore.getInstance("JKS");
trustStore.load(new FileInputStream("conf/trustStore.jks"), "changeme".toCharArray());

SSLContext sslContext = SSLContext.getInstance("SSL");

KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
keyManagerFactory.init(keyStore, "changeme".toCharArray());

TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
trustManagerFactory.init(trustStore);

TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

sslContext.init(keyManagerFactory.getKeyManagers(), trustManagers, new SecureRandom());
sslContext.getDefaultSSLParameters().setNeedClientAuth(true);

OkHttpClient.Builder okhttpClientBuilder = new OkHttpClient.Builder();
okhttpClientBuilder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustManagers[0]);

InfluxDB influxDB = InfluxDBFactory.connect("https://proxy_host:9086", okhttpClientBuilder);
```

InfluxDB (v1.6.2) does not have built-in support for client certificate ssl authentication.
SSL must be handled by http proxy such as Haproxy, nginx...

package org.influxdb.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.influxdb.InfluxDBIOException;

import okhttp3.HttpUrl;

/**
 * A resolver that periodically resolve the hostname.
 *
 * @author hoan.le [at] bonitoo.io
 *
 */

class HostAddressResolver {

  private final AtomicReference<InetAddress> atomicHostAddress = new AtomicReference<>();
  private final ScheduledExecutorService hostAddressUpdater = Executors
      .newSingleThreadScheduledExecutor(Executors.defaultThreadFactory());

  HostAddressResolver(final String url, final int period, final TimeUnit timeUnit) {

    atomicHostAddress.set(parseHostAddress(url));

    hostAddressUpdater.scheduleAtFixedRate(() -> {
      atomicHostAddress.set(parseHostAddress(url));
    }, period, period, timeUnit);
  }

  InetAddress get() {
    return atomicHostAddress.get();
  }

  void set(final InetAddress inetAddress) {
    atomicHostAddress.set(inetAddress);
  }

  void shutdown() {
    hostAddressUpdater.shutdown();
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

}

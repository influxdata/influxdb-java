package org.influxdb.impl;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;


/**
 * Test cases for HostAddressResolver
 *
 * @author hoan.le [at] bonitoo.io
 *
 */

@RunWith(JUnitPlatform.class)
public class HostAddressResolverTest {

  @Test
  public void testInit() {
    HostAddressResolver resolver = new HostAddressResolver("http://localhost:8086", 2, TimeUnit.SECONDS);
    Assertions.assertEquals("127.0.0.1", resolver.get().getHostAddress());
    resolver.shutdown();
  }
  
  @Test
  public void testSchedule() throws InterruptedException {
    HostAddressResolver resolver = new HostAddressResolver("http://localhost:8086", 2, TimeUnit.SECONDS);
    Assertions.assertEquals("127.0.0.1", resolver.get().getHostAddress());
    resolver.set(null);
    Assertions.assertNull(resolver.get());
    Thread.sleep(2500);
    Assertions.assertEquals("127.0.0.1", resolver.get().getHostAddress());
    resolver.shutdown();
  }
}

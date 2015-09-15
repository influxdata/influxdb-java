package org.influxdb;

import ch.qos.logback.classic.pattern.ThrowableProxyConverter;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.CoreConstants;

public class WhitespaceConverter extends ThrowableProxyConverter {
   @Override
   protected String throwableProxyToString(IThrowableProxy tp) {
      return CoreConstants.LINE_SEPARATOR + super.throwableProxyToString(tp) + CoreConstants.LINE_SEPARATOR;
   }
}

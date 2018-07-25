package org.influxdb.msgpack;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import okhttp3.ResponseBody;
import retrofit2.Converter;
import retrofit2.Retrofit;

/**
 * A Retrofit Convertor Factory for MessagePack response.
 *
 * @author hoan.le [at] bonitoo.io
 *
 */
public class MessagePackConverterFactory extends Converter.Factory {
  public static MessagePackConverterFactory create() {
    return new MessagePackConverterFactory();
  }

  @Override
  public Converter<ResponseBody, ?> responseBodyConverter(final Type type, final Annotation[] annotations,
      final Retrofit retrofit) {
    return new MessagePackResponseBodyConverter();
  }
}

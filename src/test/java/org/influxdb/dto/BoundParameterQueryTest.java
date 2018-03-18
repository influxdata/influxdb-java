package org.influxdb.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.influxdb.dto.BoundParameterQuery.QueryBuilder;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;

/**
 * Test for the BoundParameterQuery DTO.
 */
@RunWith(JUnitPlatform.class)
public class BoundParameterQueryTest {

  @Test
  public void testGetParameterJsonWithUrlEncoded() throws IOException {
    BoundParameterQuery query = QueryBuilder.newQuery("SELECT * FROM abc WHERE integer > $i"
        + "AND double = $d AND bool = $bool AND string = $string AND other = $object")
        .forDatabase("foobar")
        .bind("i", 0)
        .bind("d", 1.0)
        .bind("bool", true)
        .bind("string", "test")
        .bind("object", new Object())
        .create();
    
    Moshi moshi = new Moshi.Builder().build();
    JsonAdapter<Point> adapter = moshi.adapter(Point.class);
    Point point = adapter.fromJson(decode(query.getParameterJsonWithUrlEncoded()));
    Assert.assertEquals(0, point.i);
    Assert.assertEquals(1.0, point.d, 0.0);
    Assert.assertEquals(true, point.bool);
    Assert.assertEquals("test", point.string);
    Assert.assertTrue(point.object.matches("java.lang.Object@[a-z0-9]+"));
  }

  @Test
  public void testEqualsAndHashCode() {
    String stringA0 = "SELECT * FROM foobar WHERE a = $a";
    String stringA1 = "SELECT * FROM foobar WHERE a = $a";
    String stringB0 = "SELECT * FROM foobar WHERE b = $b";

    Query queryA0 = QueryBuilder.newQuery(stringA0)
        .forDatabase(stringA0)
        .bind("a", 0)
        .create();
    Query queryA1 = QueryBuilder.newQuery(stringA1)
        .forDatabase(stringA1)
        .bind("a", 0)
        .create();
    Query queryA2 = QueryBuilder.newQuery(stringA1)
        .forDatabase(stringA1)
        .bind("a", 10)
        .create();
    Query queryB0 = QueryBuilder.newQuery(stringB0)
        .forDatabase(stringB0)
        .bind("b", 10)
        .create();

    assertThat(queryA0).isEqualTo(queryA0);
    assertThat(queryA0).isEqualTo(queryA1);
    assertThat(queryA0).isNotEqualTo(queryA2);
    assertThat(queryA0).isNotEqualTo(queryB0);
    assertThat(queryA0).isNotEqualTo("foobar");

    assertThat(queryA0.hashCode()).isEqualTo(queryA1.hashCode());
    assertThat(queryA0.hashCode()).isNotEqualTo(queryB0.hashCode());
  }

  private static String decode(String str) throws UnsupportedEncodingException {
    return URLDecoder.decode(str, StandardCharsets.UTF_8.toString());
  }
  
  private static class Point {
    int i;
    double d;
    String string;
    Boolean bool;
    String object;
  }
}

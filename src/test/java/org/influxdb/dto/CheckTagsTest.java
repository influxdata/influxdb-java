package org.influxdb.dto;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.utils.CheckTags;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class CheckTagsTest {
  @Test
  public void TagNameNewLineTest() {
    final String tagname = "ma\ndrid";
    final String tagname1 = "madrid\n";
    final String tagname2 = "\nmadrid";
    
    Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(tagname,"city").addField("a", 1.0).build();
    Assert.assertFalse(point.getTags().containsKey("madrid"));
    Assert.assertFalse(point.getTags().containsKey("mad\nrid"));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname1));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname2));
  }
  @Test
  public void TagNameCarriageReturnTest() {
    final String tagname = "ma\rdrid";
    final String tagname1 = "madrid\r";
    final String tagname2 = "\rmadrid";
    Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(tagname,"city").addField("a", 1.0).build();
    Assert.assertFalse(point.getTags().containsKey("madrid"));
    Assert.assertFalse(point.getTags().containsKey("mad\rrid"));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname1));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname2));
  }
  @Test
  public void TagNameSymbolTest() {
    final String tagname = "$cost";
    final String tagname1 = "!cost";
    Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(tagname,"$15").addField("a", 1.0).build();
    Assert.assertFalse(point.getTags().containsKey("cost"));
    Assert.assertTrue(point.getTags().containsKey("$cost"));
    Assert.assertTrue(CheckTags.isTagNameLegal(tagname));
    Assert.assertTrue(CheckTags.isTagNameLegal(tagname1));
    final HashMap<String, String> map = new HashMap<>();
    map.put("$cost","$15");
    map.put("$mortgage","$34,000");
    map.put("%interest","65%");
    map.put("@email","startrek@cbs.com");
    Point.Builder point1 = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(map).addField("a", 1.0);
    Assertions.assertThat(point1.build().getTags().values().equals(map.values()));
  }
  @Test
  public void BatchPointsTagTest() {
    final HashMap<String, String> map = new HashMap<>();
    map.put("$cost","$15");
    map.put("$mortgage","$34,000");
    map.put("%interest","65%");
    map.put("@email","startrek@cbs.com");
    Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).addField("a", 1.0).build();
    BatchPoints.Builder points = BatchPoints.builder()
        .tag("$cost","$15").tag("$mortgage","$34,000")
        .tag("%interest","65%").tag("@email","startrek@cbs.com").point(point);
    Assertions.assertThat(points.build().getPoints().get(0).getTags().equals(map.values()));
    map.put("#phone","1-555-0101");
    Assertions.assertThat(!points.build().getPoints().get(0).getTags().equals(map.values()));
  }
  @Test
  public void TagNameHyphenTest() {
    final String tagname = "-cost";
    final String tagname1 = "bushel-cost";
    Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(tagname,"$15").addField("a", 1.0).build();
    Assert.assertTrue(point.getTags().containsKey("-cost"));
    Assert.assertFalse(point.getTags().containsKey("cost"));
    Assert.assertTrue(CheckTags.isTagNameLegal(tagname));
    Assert.assertTrue(CheckTags.isTagNameLegal(tagname1));
  }
  @Test
  public void TagNameUnderscoreTest() {
    final String tagname = "_cost_";
    final String tagname1 = "bushel_cost";
    Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(tagname,"$15").addField("a", 1.0).build();
    Assert.assertTrue(point.getTags().containsKey("_cost_"));
    Assert.assertTrue(CheckTags.isTagNameLegal(tagname));
    Assert.assertTrue(CheckTags.isTagNameLegal(tagname1));
  }
  @Test
  public void TagValueASCIITest() {
    final String tagvalue = "$15";
    final String tagvalue1 = "$34,000";
    final String tagvalue2 = "65%";
    final String tagvalue3 = "startrek@cbs.com";
    final String tagvalue4 = "%SYSTEM_VARARG%";
    Point.Builder point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("cost",tagvalue).addField("a", 1.0);
    Assertions.assertThat(point.build().lineProtocol()).isEqualTo("test,cost=$15 a=1.0 1");
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue));
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue1));
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue2));
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue3));
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue4));
    Assert.assertFalse(CheckTags.isTagValueLegal("ąćę"));
    final HashMap<String, String> map = new HashMap<>();
    map.put("cost",tagvalue);
    map.put("mortgage",tagvalue1);
    map.put("interest",tagvalue2);
    map.put("email",tagvalue3);
    Point.Builder point1 = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(map).addField("a", 1.0);
    Assertions.assertThat(point1.build().getTags().values().equals(map.values()));
    map.put("phone","1-555-0101");
    Assertions.assertThat(!point1.build().getTags().values().equals(map.values()));
  
  }
  @Test
  public void TagsNullOrEmpty(){
    final String tagname = null;
    final String tagvalue = null;
    final String tagvalue1 = "";
    
    Assert.assertThrows(NullPointerException.class, ()-> {
      Point.Builder point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("cost",tagvalue).addField("a", 1.0);
    });
  
    Assert.assertThrows(NullPointerException.class, ()-> {
      Point.Builder point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(tagname,"whistle while you work").addField("a", 1.0);
    });
    
    Point.Builder point1 = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag("cost",tagvalue1).addField("a", 1.0);
    Assert.assertTrue(point1.build().getTags().isEmpty());
  
  }
}

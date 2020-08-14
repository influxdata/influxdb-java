package org.influxdb.dto;

import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.influxdb.dto.utils.CheckTags;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class CheckTagsTest {
  @Test
  public void TagNameNewLineTest() {
    final String tagname = "mad\ndrid";
    final String tagname1 = "maddrid\n";
    final String tagname2 = "\nmaddrid";
    
    Point point = Point.measurement("test").time(1, TimeUnit.NANOSECONDS).tag(tagname,"city").addField("a", 1.0).build();
    Assert.assertFalse(point.getTags().containsKey("madrid"));
    Assert.assertFalse(point.getTags().containsKey("mad\nrid"));
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

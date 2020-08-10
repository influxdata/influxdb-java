package org.influxdb.dto;

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
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname1));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname2));
  }
  @Test
  public void TagNameSymbolTest() {
    final String tagname = "$cost";
    final String tagname1 = "!cost";
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname));
    Assert.assertFalse(CheckTags.isTagNameLegal(tagname1));
  }
  @Test
  public void TagNameHyphenTest() {
    final String tagname = "-cost";
    final String tagname1 = "bushel-cost";
    Assert.assertTrue(CheckTags.isTagNameLegal(tagname));
    Assert.assertTrue(CheckTags.isTagNameLegal(tagname1));
  }
  @Test
  public void TagNameUnderscoreTest() {
    final String tagname = "_cost_";
    final String tagname1 = "bushel_cost";
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
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue));
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue1));
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue2));
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue3));
    Assert.assertTrue(CheckTags.isTagValueLegal(tagvalue4));
  }
}

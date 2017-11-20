package org.influxdb.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class PreconditionsTest {

  @Test
  public void testCheckNonEmptyString1() {
    final String string = "foo";
    Preconditions.checkNonEmptyString(string, "string");
  }

  @Test
  public void testCheckNonEmptyString2() {
    final String string = "";
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      Preconditions.checkNonEmptyString(string, "string");
		});
  }

  @Test
  public void testCheckNonEmptyString3() {
    final String string = null;
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      Preconditions.checkNonEmptyString(string, "string");
		});
  }

  @Test
  public void testCheckPositiveNumber1() {
    final Number number = 42;
    Preconditions.checkPositiveNumber(number, "number");
  }

  @Test
  public void testCheckPositiveNumber2() {
    final Number number = 0;
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      Preconditions.checkPositiveNumber(number, "number");
		});
  }

  @Test
  public void testCheckPositiveNumber3() {
    final Number number = null;
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      Preconditions.checkPositiveNumber(number, "number");
		});
  }

}
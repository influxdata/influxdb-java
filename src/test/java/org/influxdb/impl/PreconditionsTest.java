package org.influxdb.impl;

import org.junit.Test;

public class PreconditionsTest {

  @Test
  public void checkNonEmptyString1() {
    final String string = "foo";
    Preconditions.checkNonEmptyString(string, "string");
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkNonEmptyString2() {
    final String string = "";
    Preconditions.checkNonEmptyString(string, "string");
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkNonEmptyString3() {
    final String string = null;
    Preconditions.checkNonEmptyString(string, "string");
  }

  @Test
  public void checkPositiveNumber1() {
    final Number number = 42;
    Preconditions.checkPositiveNumber(number, "number");
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkPositiveNumber2() {
    final Number number = 0;
    Preconditions.checkPositiveNumber(number, "number");
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkPositiveNumber3() {
    final Number number = null;
    Preconditions.checkPositiveNumber(number, "number");
  }

}
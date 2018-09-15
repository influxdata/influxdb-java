package org.influxdb.querybuilder;

public final class Operations {

  private Operations() {
  }

  public static final String EQ = "=";
  public static final String NE = "!=";
  public static final String NEQ = "<>";
  public static final String LT = "<";
  public static final String LTE = "<=";
  public static final String GT = ">";
  public static final String GTE = ">=";
  public static final String EQR = "=~";
  public static final String NER = "!~";
  public static final String ADD = "+";
  public static final String SUB = "-";
  public static final String MUL = "*";
}

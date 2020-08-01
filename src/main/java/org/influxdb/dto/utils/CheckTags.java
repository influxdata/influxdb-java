package org.influxdb.dto.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {Purpose of This Type}.
 *
 * {Other Notes Relating to This Type (Optional)}
 *
 * @author Brenton Poke
 *
 */
public final class CheckTags {
  static final String nameRegex = "([a-zA-Z0-9-_]+)";
  static final String valueRegex = "[\\x00-\\x7F]+";
  static final Pattern namePattern = Pattern.compile(nameRegex, Pattern.MULTILINE);
  static final Pattern valuePattern = Pattern.compile(valueRegex, Pattern.MULTILINE);
  
  /**
   * Check a single tag's name according to the corresponding regular expression.
   *
   * @param name
   *            the tag name
   * @return Boolean indicating that the tag name is legal
   *
   */
  
  public static Boolean isTagNameLegal(String name){
    final Matcher matcher = namePattern.matcher(name);
    return matcher.matches();
  }
  
  /**
   * Check a single tag's name according to the corresponding regular expression.
   *
   * @param value
   *            the tag value
   * @return Boolean indicating that the tag value is legal
   *
   */
  public static Boolean isTagValueLegal(String value){
    final Matcher matcher = valuePattern.matcher(value);
    return matcher.matches();
  }
  
}
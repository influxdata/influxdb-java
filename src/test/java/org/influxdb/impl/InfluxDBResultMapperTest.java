/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 azeti Networks AG (<info@azeti.net>)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.influxdb.impl;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDBMapperException;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * @author fmachado
 */
@RunWith(JUnitPlatform.class)
public class InfluxDBResultMapperTest {

  InfluxDBResultMapper mapper = new InfluxDBResultMapper();

  @Test
  public void testToPOJO_HappyPath() {
    // Given...
    List<String> columnList = Arrays.asList("time", "uuid");
    List<Object> firstSeriesResult = Arrays.asList(Instant.now().toEpochMilli(), UUID.randomUUID().toString());

    QueryResult.Series series = new QueryResult.Series();
    series.setColumns(columnList);
    series.setName("CustomMeasurement");
    series.setValues(Arrays.asList(firstSeriesResult));

    QueryResult.Result internalResult = new QueryResult.Result();
    internalResult.setSeries(Arrays.asList(series));

    QueryResult queryResult = new QueryResult();
    queryResult.setResults(Arrays.asList(internalResult));

    //When...
    List<MyCustomMeasurement> myList = mapper.toPOJO(queryResult, MyCustomMeasurement.class);

    // Then...
    Assertions.assertEquals(1, myList.size(), "there must be one entry in the result list");
  }

	@Test
	public void testThrowExceptionIfMissingAnnotation() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			mapper.throwExceptionIfMissingAnnotation(String.class);
		});
	}

	@Test
	public void testThrowExceptionIfError_InfluxQueryResultHasError() {
		QueryResult queryResult = new QueryResult();
		queryResult.setError("main queryresult error");

		Assertions.assertThrows(InfluxDBMapperException.class, () -> {
			mapper.throwExceptionIfResultWithError(queryResult);
		});
	}

	@Test
	public void testThrowExceptionIfError_InfluxQueryResultSeriesHasError() {
		QueryResult queryResult = new QueryResult();

		QueryResult.Result seriesResult = new QueryResult.Result();
		seriesResult.setError("series error");

		queryResult.setResults(Arrays.asList(seriesResult));

		Assertions.assertThrows(InfluxDBMapperException.class, () -> {
			mapper.throwExceptionIfResultWithError(queryResult);
		});
	}

	@Test
	public void testGetMeasurementName_testStateMeasurement() {
		Assertions.assertEquals("CustomMeasurement", mapper.getMeasurementName(MyCustomMeasurement.class));
	}

	@Test
	public void testParseSeriesAs_testTwoValidSeries() {
	  // Given...
		mapper.cacheMeasurementClass(MyCustomMeasurement.class);

		List<String> columnList = Arrays.asList("time", "uuid");

		List<Object> firstSeriesResult = Arrays.asList(Instant.now().toEpochMilli(), UUID.randomUUID().toString());
		List<Object> secondSeriesResult = Arrays.asList(Instant.now().plusSeconds(1).toEpochMilli(), UUID.randomUUID().toString());

		QueryResult.Series series = new QueryResult.Series();
		series.setColumns(columnList);
		series.setValues(Arrays.asList(firstSeriesResult, secondSeriesResult));

		//When...
		List<MyCustomMeasurement> result = new LinkedList<>();
		mapper.parseSeriesAs(series, MyCustomMeasurement.class, result);

		//Then...
		Assertions.assertTrue(result.size() == 2, "there must be two series in the result list");

		Assertions.assertEquals(firstSeriesResult.get(0), result.get(0).time.toEpochMilli(), "Field 'time' (1st series) is not valid");
		Assertions.assertEquals(firstSeriesResult.get(1), result.get(0).uuid, "Field 'uuid' (1st series) is not valid");

		Assertions.assertEquals(secondSeriesResult.get(0), result.get(1).time.toEpochMilli(), "Field 'time' (2nd series) is not valid");
		Assertions.assertEquals(secondSeriesResult.get(1), result.get(1).uuid, "Field 'uuid' (2nd series) is not valid");
	}

	@Test
	public void testParseSeriesAs_testNonNullAndValidValues() {
	  // Given...
		mapper.cacheMeasurementClass(MyCustomMeasurement.class);

		List<String> columnList = Arrays.asList("time", "uuid",
			"doubleObject", "longObject", "integerObject",
			"doublePrimitive", "longPrimitive", "integerPrimitive",
			"booleanObject", "booleanPrimitive");

		// InfluxDB client returns the time representation as Double.
		Double now = Long.valueOf(System.currentTimeMillis()).doubleValue();
		String uuidAsString = UUID.randomUUID().toString();

		// InfluxDB client returns any number as Double.
		// See https://github.com/influxdata/influxdb-java/issues/153#issuecomment-259681987
		// for more information.
		List<Object> seriesResult = Arrays.asList(now, uuidAsString,
			new Double("1.01"), new Double("2"), new Double("3"),
			new Double("1.01"), new Double("4"), new Double("5"),
			"false", "true");

		QueryResult.Series series = new QueryResult.Series();
		series.setColumns(columnList);
		series.setValues(Arrays.asList(seriesResult));

		//When...
		List<MyCustomMeasurement> result = new LinkedList<>();
		mapper.parseSeriesAs(series, MyCustomMeasurement.class, result);

		//Then...
		MyCustomMeasurement myObject = result.get(0);
		Assertions.assertEquals(now.longValue(), myObject.time.toEpochMilli(), "field 'time' does not match");
		Assertions.assertEquals(uuidAsString, myObject.uuid, "field 'uuid' does not match");

		Assertions.assertEquals(asDouble(seriesResult.get(2)), myObject.doubleObject, "field 'doubleObject' does not match");
		Assertions.assertEquals(new Long(asDouble(seriesResult.get(3)).longValue()), myObject.longObject, "field 'longObject' does not match");
		Assertions.assertEquals(new Integer(asDouble(seriesResult.get(4)).intValue()), myObject.integerObject, "field 'integerObject' does not match");

		Assertions.assertTrue(
			Double.compare(asDouble(seriesResult.get(5)).doubleValue(), myObject.doublePrimitive) == 0,
			"field 'doublePrimitive' does not match");

		Assertions.assertTrue(
			Long.compare(asDouble(seriesResult.get(6)).longValue(), myObject.longPrimitive) == 0,
			"field 'longPrimitive' does not match");

		Assertions.assertTrue(
			Integer.compare(asDouble(seriesResult.get(7)).intValue(), myObject.integerPrimitive) == 0,
			"field 'integerPrimitive' does not match");

		Assertions.assertEquals(
			Boolean.valueOf(String.valueOf(seriesResult.get(8))), myObject.booleanObject,
			"field 'booleanObject' does not match");

		Assertions.assertEquals(
			Boolean.valueOf(String.valueOf(seriesResult.get(9))).booleanValue(), myObject.booleanPrimitive,
			"field 'booleanPrimitive' does not match");
	}

	Double asDouble(Object obj) {
		return (Double) obj;
	}

	@Test
	public void testFieldValueModified_DateAsISO8601() {
	  // Given...
		mapper.cacheMeasurementClass(MyCustomMeasurement.class);

		List<String> columnList = Arrays.asList("time");
		List<Object> firstSeriesResult = Arrays.asList("2017-06-19T09:29:45.655123Z");

		QueryResult.Series series = new QueryResult.Series();
		series.setColumns(columnList);
		series.setValues(Arrays.asList(firstSeriesResult));

		//When...
		List<MyCustomMeasurement> result = new LinkedList<>();
		mapper.parseSeriesAs(series, MyCustomMeasurement.class, result);

		//Then...
		Assertions.assertTrue(result.size() == 1);
	}

	@Test
	public void testFieldValueModified_DateAsInteger() {
	  // Given...
		mapper.cacheMeasurementClass(MyCustomMeasurement.class);

		List<String> columnList = Arrays.asList("time");
		List<Object> firstSeriesResult = Arrays.asList(1_000);

		QueryResult.Series series = new QueryResult.Series();
		series.setColumns(columnList);
		series.setValues(Arrays.asList(firstSeriesResult));

		//When...
		List<MyCustomMeasurement> result = new LinkedList<>();
		mapper.parseSeriesAs(series, MyCustomMeasurement.class, result);

		//Then...
		Assertions.assertTrue(result.size() == 1);
	}

	@Test
	public void testUnsupportedField() {
	  // Given...
		mapper.cacheMeasurementClass(MyPojoWithUnsupportedField.class);

		List<String> columnList = Arrays.asList("bar");
		List<Object> firstSeriesResult = Arrays.asList("content representing a Date");

		QueryResult.Series series = new QueryResult.Series();
		series.setColumns(columnList);
		series.setValues(Arrays.asList(firstSeriesResult));

		//When...
		List<MyPojoWithUnsupportedField> result = new LinkedList<>();
		Assertions.assertThrows(InfluxDBMapperException.class, () -> {
			mapper.parseSeriesAs(series, MyPojoWithUnsupportedField.class, result);
		});
	}

	/**
	 * https://github.com/influxdata/influxdb/issues/7596 for more information.
	 */
  @Test
  public void testToPOJO_SeriesFromQueryResultIsNull() {
    // Given...
    mapper.cacheMeasurementClass(MyCustomMeasurement.class);

    QueryResult.Result internalResult = new QueryResult.Result();
    internalResult.setSeries(null);

    QueryResult queryResult = new QueryResult();
    queryResult.setResults(Arrays.asList(internalResult));

    // When...
    List<MyCustomMeasurement> myList = mapper.toPOJO(queryResult, MyCustomMeasurement.class);

    // Then...
    Assertions.assertTrue( myList.isEmpty(), "there must NO entry in the result list");
  }

  @Test
  public void testToPOJO_QueryResultCreatedByGroupByClause() {
    // Given...
    mapper.cacheMeasurementClass(GroupByCarrierDeviceOS.class);

    List<String> columnList = Arrays.asList("time", "median", "min", "max");

    // InfluxDB client returns the time representation as Double.
    Double now = Long.valueOf(System.currentTimeMillis()).doubleValue();

    List<Object> firstSeriesResult = Arrays.asList(now, new Double("233.8"), new Double("0.0"),
      new Double("3090744.0"));
    // When the "GROUP BY" clause is used, "tags" are returned as Map<String,String>
    Map<String, String> firstSeriesTagMap = new HashMap<>();
    firstSeriesTagMap.put("CARRIER", "000/00");
    firstSeriesTagMap.put("DEVICE_OS_VERSION", "4.4.2");

    List<Object> secondSeriesResult = Arrays.asList(now, new Double("552.0"), new Double("135.0"),
      new Double("267705.0"));
    Map<String, String> secondSeriesTagMap = new HashMap<>();
    secondSeriesTagMap.put("CARRIER", "000/01");
    secondSeriesTagMap.put("DEVICE_OS_VERSION", "9.3.5");

    QueryResult.Series firstSeries = new QueryResult.Series();
    firstSeries.setColumns(columnList);
    firstSeries.setValues(Arrays.asList(firstSeriesResult));
    firstSeries.setTags(firstSeriesTagMap);
    firstSeries.setName("tb_network");

    QueryResult.Series secondSeries = new QueryResult.Series();
    secondSeries.setColumns(columnList);
    secondSeries.setValues(Arrays.asList(secondSeriesResult));
    secondSeries.setTags(secondSeriesTagMap);
    secondSeries.setName("tb_network");

    QueryResult.Result internalResult = new QueryResult.Result();
    internalResult.setSeries(Arrays.asList(firstSeries, secondSeries));

    QueryResult queryResult = new QueryResult();
    queryResult.setResults(Arrays.asList(internalResult));

    // When...
    List<GroupByCarrierDeviceOS> myList = mapper.toPOJO(queryResult, GroupByCarrierDeviceOS.class);

    // Then...
    GroupByCarrierDeviceOS firstGroupByEntry = myList.get(0);
    Assertions.assertEquals("000/00", firstGroupByEntry.carrier, "field 'carrier' does not match");
    Assertions.assertEquals("4.4.2", firstGroupByEntry.deviceOsVersion, "field 'deviceOsVersion' does not match");

    GroupByCarrierDeviceOS secondGroupByEntry = myList.get(1);
    Assertions.assertEquals("000/01", secondGroupByEntry.carrier, "field 'carrier' does not match");
    Assertions.assertEquals("9.3.5", secondGroupByEntry.deviceOsVersion, "field 'deviceOsVersion' does not match");
  }

  @Test
  public void testToPOJO_ticket363() {
    // Given...
    mapper.cacheMeasurementClass(MyCustomMeasurement.class);

    List<String> columnList = Arrays.asList("time");
    List<Object> firstSeriesResult = Arrays.asList("2000-01-01T00:00:00.000000001Z");

    QueryResult.Series series = new QueryResult.Series();
    series.setColumns(columnList);
    series.setValues(Arrays.asList(firstSeriesResult));

    // When...
    List<MyCustomMeasurement> result = new LinkedList<>();
    mapper.parseSeriesAs(series, MyCustomMeasurement.class, result);

    // Then...
    Assertions.assertEquals(1, result.size(), "incorrect number of elemets");
    Assertions.assertEquals(1, result.get(0).time.getNano(), "incorrect value for the nanoseconds field");
  }

	@Test
	void testToPOJO_Precision() {
		// Given...
		mapper.cacheMeasurementClass(MyCustomMeasurement.class);

		List<String> columnList = Arrays.asList("time");
		List<Object> firstSeriesResult = Arrays.asList(1_500_000L);

		QueryResult.Series series = new QueryResult.Series();
		series.setName("CustomMeasurement");
		series.setColumns(columnList);
		series.setValues(Arrays.asList(firstSeriesResult));

		QueryResult.Result internalResult = new QueryResult.Result();
		internalResult.setSeries(Arrays.asList(series));

		QueryResult queryResult = new QueryResult();
		queryResult.setResults(Arrays.asList(internalResult));

		// When...
		List<MyCustomMeasurement> result = mapper.toPOJO(queryResult, MyCustomMeasurement.class, TimeUnit.SECONDS);

		// Then...
		Assertions.assertEquals(1, result.size(), "incorrect number of elements");
		Assertions.assertEquals(1_500_000_000L, result.get(0).time.toEpochMilli(), "incorrect value for the millis field");
	}

	@Test
	void testToPOJO_SetMeasureName() {
		// Given...
		mapper.cacheMeasurementClass(MyCustomMeasurement.class);

		List<String> columnList = Arrays.asList("uuid");
		List<Object> firstSeriesResult = Arrays.asList(UUID.randomUUID().toString());

		QueryResult.Series series = new QueryResult.Series();
		series.setName("MySeriesName");
		series.setColumns(columnList);
		series.setValues(Arrays.asList(firstSeriesResult));

		QueryResult.Result internalResult = new QueryResult.Result();
		internalResult.setSeries(Arrays.asList(series));

		QueryResult queryResult = new QueryResult();
		queryResult.setResults(Arrays.asList(internalResult));

		//When...
		List<MyCustomMeasurement> result =
				mapper.toPOJO(queryResult, MyCustomMeasurement.class, "MySeriesName");

		//Then...
		Assertions.assertTrue(result.size() == 1);
	}

	@Measurement(name = "CustomMeasurement")
	static class MyCustomMeasurement {

		@Column(name = "time")
		private Instant time;

		@Column(name = "uuid")
		private String uuid;

		@Column(name = "doubleObject")
		private Double doubleObject;

		@Column(name = "longObject")
		private Long longObject;

		@Column(name = "integerObject")
		private Integer integerObject;

		@Column(name = "doublePrimitive")
		private double doublePrimitive;

		@Column(name = "longPrimitive")
		private long longPrimitive;

		@Column(name = "integerPrimitive")
		private int integerPrimitive;

		@Column(name = "booleanObject")
		private Boolean booleanObject;

		@Column(name = "booleanPrimitive")
		private boolean booleanPrimitive;

		@SuppressWarnings("unused")
		private String nonColumn1;

		@SuppressWarnings("unused")
		private Random rnd;

		@Override
		public String toString() {
			return "MyCustomMeasurement [time=" + time + ", uuid=" + uuid + ", doubleObject=" + doubleObject + ", longObject=" + longObject
				+ ", integerObject=" + integerObject + ", doublePrimitive=" + doublePrimitive + ", longPrimitive=" + longPrimitive
				+ ", integerPrimitive=" + integerPrimitive + ", booleanObject=" + booleanObject + ", booleanPrimitive=" + booleanPrimitive + "]";
		}
	}

	@Measurement(name = "foo")
	static class MyPojoWithUnsupportedField {

		@Column(name = "bar")
		private Date myDate;
	}

  /**
   * Class created based on example from https://github.com/influxdata/influxdb-java/issues/343
   */
  @Measurement(name = "tb_network")
  static class GroupByCarrierDeviceOS {

    @Column(name = "time")
    private Instant time;

    @Column(name = "CARRIER", tag = true)
    private String carrier;

    @Column(name = "DEVICE_OS_VERSION", tag = true)
    private String deviceOsVersion;

    @Column(name = "median")
    private Double median;

    @Column(name = "min")
    private Double min;

    @Column(name = "max")
    private Double max;

    @Override
    public String toString() {
      return "GroupByCarrierDeviceOS [time=" + time + ", carrier=" + carrier + ", deviceOsVersion=" + deviceOsVersion
        + ", median=" + median + ", min=" + min + ", max=" + max + "]";
    }
  }
}
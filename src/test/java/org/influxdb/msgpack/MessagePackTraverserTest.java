package org.influxdb.msgpack;

import java.util.Iterator;
import java.util.List;

import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
@EnabledIfEnvironmentVariable(named = "INFLUXDB_VERSION", matches = "1\\.6|1\\.5|1\\.4")
public class MessagePackTraverserTest {
  
  @Test
  public void testTraverseMethod() {
    MessagePackTraverser traverser = new MessagePackTraverser();
    
    /*  a json-like view of msgpack_1.bin

    {"results":[{"statement_id":0,"series":[{"name":"disk","columns":["time","atag","free","used"],
          "values":[[(5,0x00005b556c-252f-23-6438),"a",1,60],[(5,0x00005b556c-252f-23-6438),"b",2,70]],"partial":true}],"partial":true}]}
    {"results":[{"statement_id":0,"series":[{"name":"disk","columns":["time","atag","free","used"],"values":[[(5,0x00005b556c-252f-23-6438),"c",3,80]]}]}]}

    */

    Iterator<QueryResult> iter = traverser.traverse(MessagePackTraverserTest.class.getResourceAsStream("msgpack_1.bin")).iterator();
    assertTrue(iter.hasNext());
    QueryResult result = iter.next();
    List<List<Object>> values = result.getResults().get(0).getSeries().get(0).getValues();
    Assertions.assertEquals(2, values.size());
    
    assertEquals(1532325083803052600L, values.get(0).get(0));
    assertEquals("b", values.get(1).get(1));
    
    assertTrue(iter.hasNext());
    result = iter.next();
    values = result.getResults().get(0).getSeries().get(0).getValues();
    Assertions.assertEquals(1, values.size());
    assertEquals(3, values.get(0).get(2));
    
    assertFalse(iter.hasNext());
  }
  
  @Test
  public void testParseMethodOnNonEmptyResult() {
    MessagePackTraverser traverser = new MessagePackTraverser();
    /* a json-like view of msgpack_2.bin

    {"results":[{"statement_id":0,"series":[{"name":"measurement_957996674028300","columns":["time","device","foo"],
          "values":[[(5,0x000058-797a00000),"one",1.0],[(5,0x000058-79-78100000),"two",2.0],[(5,0x000058-79-6a200000),"three",3.0]]}]}]}
    */
    QueryResult queryResult = traverser.parse(MessagePackTraverserTest.class.getResourceAsStream("msgpack_2.bin"));
    List<List<Object>> values = queryResult.getResults().get(0).getSeries().get(0).getValues();
    Assertions.assertEquals(3, values.size());
    assertEquals(1485273600000000000L, values.get(0).get(0));
    assertEquals("two", values.get(1).get(1));
    assertEquals(3.0, values.get(2).get(2));
  }
  
  @Test
  public void testParseMethodOnEmptyResult() {
    MessagePackTraverser traverser = new MessagePackTraverser();
    /* a json-like view of msgpack_3.bin

      {"results":[{"statement_id":0,"series":[]}]}
    
    */
    QueryResult queryResult = traverser.parse(MessagePackTraverserTest.class.getResourceAsStream("msgpack_3.bin"));
    System.out.println();
    assertNull(queryResult.getResults().get(0).getSeries());
    
  }
}

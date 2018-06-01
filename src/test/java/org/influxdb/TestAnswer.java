package org.influxdb;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public abstract class TestAnswer implements Answer<Object> {

  Map<String, Object> params = new HashMap<>();
  
  protected abstract void check(InvocationOnMock invocation);

  @Override
  public Object answer(InvocationOnMock invocation) throws Throwable {
    check(invocation);
    //call only non-abstract real method 
    if (Modifier.isAbstract(invocation.getMethod().getModifiers())) {
      return null;
    } else {
      return invocation.callRealMethod();
    }
  }
  
}
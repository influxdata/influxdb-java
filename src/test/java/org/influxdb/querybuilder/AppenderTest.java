package org.influxdb.querybuilder;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AppenderTest {

    @Test
    public void testJoinAndAppend() {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT test1,test2 FROM foo WHERE ");
        Appender.joinAndAppend(builder, " AND ", Collections.singletonList(eq("testval", "test1")));
        assertEquals("SELECT test1,test2 FROM foo WHERE testval='test1'",builder.toString());
    }

}

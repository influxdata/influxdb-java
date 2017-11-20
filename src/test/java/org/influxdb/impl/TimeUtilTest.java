package org.influxdb.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
public class TimeUtilTest {
    @Test
    public void testToInfluxDBTimeFormatTest() throws Exception {
        assertThat(TimeUtil.toInfluxDBTimeFormat(1477896740020L)).isEqualTo("2016-10-31T06:52:20.020Z");
        assertThat(TimeUtil.toInfluxDBTimeFormat(1477932740005L)).isEqualTo("2016-10-31T16:52:20.005Z");
    }

    @Test
    public void testFromInfluxDBTimeFormatTest() throws Exception {
        assertThat(TimeUtil.fromInfluxDBTimeFormat("2016-10-31T06:52:20.020Z")).isEqualTo(1477896740020L);
        assertThat(TimeUtil.fromInfluxDBTimeFormat("2016-10-31T16:52:20.005Z")).isEqualTo(1477932740005L);
        assertThat(TimeUtil.fromInfluxDBTimeFormat("2016-10-31T16:52:20Z")).isEqualTo(1477932740000L);
        assertThat(TimeUtil.fromInfluxDBTimeFormat("2016-10-31T06:52:20Z")).isEqualTo(1477896740000L);
    }
}

package org.influxdb.impl;

import org.influxdb.InfluxDB;
import org.influxdb.TestUtils;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.Query;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public class InfluxDBMapperTest {

    private InfluxDB influxDB;
    private InfluxDBMapper influxDBMapper;
    final static String UDP_DATABASE = "udp";

    @Before
    public void setUp() throws Exception {
        this.influxDB = TestUtils.connectToInfluxDB();
        this.influxDBMapper = new InfluxDBMapper(influxDB);
    }

    @Test
    public void testSave() {
        ServerMeasure serverMeasure = new ServerMeasure();
        serverMeasure.setName("maverick");
        serverMeasure.setCpu(4.3d);
        serverMeasure.setHealthy(true);
        serverMeasure.setUptime(1234l);
        serverMeasure.setMemoryUtilization(new BigDecimal("34.5"));

        influxDBMapper.save(serverMeasure);

        ServerMeasure persistedMeasure = influxDBMapper.query(ServerMeasure.class).get(0);
        Assert.assertEquals(serverMeasure.getName(),persistedMeasure.getName());
        Assert.assertEquals(serverMeasure.getCpu(),persistedMeasure.getCpu(),0);
        Assert.assertEquals(serverMeasure.isHealthy(),persistedMeasure.isHealthy());
        Assert.assertEquals(serverMeasure.getUptime(),persistedMeasure.getUptime());
        Assert.assertEquals(serverMeasure.getMemoryUtilization(),persistedMeasure.getMemoryUtilization());
    }

    @After
    public void cleanUp() throws Exception {
        influxDB.query(new Query("DROP DATABASE udp",UDP_DATABASE));
    }

    @Measurement(name = "server_measure")
    private static class ServerMeasure {

        /**
         * Check the instant convertions
         */
        @Column(name = "time")
        private Instant time;

        @Column(name = "name", tag = true)
        private String name;

        @Column(name = "cpu", tag = true)
        private double cpu;

        @Column(name = "healthy")
        private boolean healthy;

        @Column(name = "min")
        private long uptime;

        @Column(name = "memory_utilization")
        private BigDecimal memoryUtilization;

        public Instant getTime() {
            return time;
        }

        public void setTime(Instant time) {
            this.time = time;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getCpu() {
            return cpu;
        }

        public void setCpu(double cpu) {
            this.cpu = cpu;
        }

        public boolean isHealthy() {
            return healthy;
        }

        public void setHealthy(boolean healthy) {
            this.healthy = healthy;
        }

        public long getUptime() {
            return uptime;
        }

        public void setUptime(long uptime) {
            this.uptime = uptime;
        }

        public BigDecimal getMemoryUtilization() {
            return memoryUtilization;
        }

        public void setMemoryUtilization(BigDecimal memoryUtilization) {
            this.memoryUtilization = memoryUtilization;
        }

    }

}

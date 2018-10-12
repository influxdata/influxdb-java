package org.influxdb.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBMapperException;
import org.influxdb.TestUtils;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.Query;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InfluxDBMapperTest {

  private InfluxDB influxDB;
  private InfluxDBMapper influxDBMapper;
  static final String UDP_DATABASE = "udp";

  @BeforeEach
  public void setUp() throws Exception {
    this.influxDB = TestUtils.connectToInfluxDB();
    this.influxDB.query(new Query("CREATE DATABASE " + UDP_DATABASE, UDP_DATABASE));
    this.influxDB.setDatabase(UDP_DATABASE);
    this.influxDBMapper = new InfluxDBMapper(influxDB);
  }

  @Test
  public void testSave() {
    ServerMeasure serverMeasure = new ServerMeasure();
    serverMeasure.setName("maverick");
    serverMeasure.setCpu(4.3d);
    serverMeasure.setHealthy(true);
    serverMeasure.setUptime(1234l);
    serverMeasure.setMemoryUtilization(new Double(34.5));

    influxDBMapper.save(serverMeasure);

    ServerMeasure persistedMeasure = influxDBMapper.query(ServerMeasure.class).get(0);
    Assert.assertEquals(serverMeasure.getName(), persistedMeasure.getName());
    Assert.assertEquals(serverMeasure.getCpu(), persistedMeasure.getCpu(), 0);
    Assert.assertEquals(serverMeasure.isHealthy(), persistedMeasure.isHealthy());
    Assert.assertEquals(serverMeasure.getUptime(), persistedMeasure.getUptime());
    Assert.assertEquals(
        serverMeasure.getMemoryUtilization(), persistedMeasure.getMemoryUtilization());
  }

  @Test
  public void testIllegalField() {
    InvalidMeasure invalidMeasure = new InvalidMeasure();
    invalidMeasure.setVal(new BigDecimal("2.3"));
    assertThrows(
        InfluxDBMapperException.class,
        () -> influxDBMapper.save(invalidMeasure),
        "Non supported field");
  }

  @AfterEach
  public void cleanUp() throws Exception {
    influxDB.query(new Query("DROP DATABASE udp", UDP_DATABASE));
  }

  @Measurement(name = "server_measure", database = UDP_DATABASE)
  static class ServerMeasure {

    /** Check the instant convertions */
    @Column(name = "time")
    private Instant time;

    @Column(name = "name", tag = true)
    private String name;

    @Column(name = "cpu")
    private double cpu;

    @Column(name = "healthy")
    private boolean healthy;

    @Column(name = "min")
    private long uptime;

    /** TODO bigdecimal unsupported? */
    @Column(name = "memory_utilization")
    private Double memoryUtilization;

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

    public Double getMemoryUtilization() {
      return memoryUtilization;
    }

    public void setMemoryUtilization(Double memoryUtilization) {
      this.memoryUtilization = memoryUtilization;
    }
  }

  @Measurement(name = "invalid_measure", database = UDP_DATABASE)
  static class InvalidMeasure {

    /** Check the instant convertions */
    @Column(name = "illegal_val")
    private BigDecimal val;

    public BigDecimal getVal() {
      return val;
    }

    public void setVal(BigDecimal val) {
      this.val = val;
    }
  }
}

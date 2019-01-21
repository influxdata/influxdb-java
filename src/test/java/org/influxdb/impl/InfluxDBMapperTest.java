package org.influxdb.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

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
    ServerMeasure serverMeasure = createMeasure();
    influxDBMapper.save(serverMeasure);

    ServerMeasure persistedMeasure = influxDBMapper.query(ServerMeasure.class).get(0);
    Assert.assertEquals(serverMeasure.getName(), persistedMeasure.getName());
    Assert.assertEquals(serverMeasure.getCpu(), persistedMeasure.getCpu(), 0);
    Assert.assertEquals(serverMeasure.isHealthy(), persistedMeasure.isHealthy());
    Assert.assertEquals(serverMeasure.getUptime(), persistedMeasure.getUptime());
    Assert.assertEquals(serverMeasure.getIp(),persistedMeasure.getIp());
    Assert.assertEquals(
        serverMeasure.getMemoryUtilization(), persistedMeasure.getMemoryUtilization());
  }

  @Test
  public void testQuery() {
    ServerMeasure serverMeasure = createMeasure();
    influxDBMapper.save(serverMeasure);

    List<ServerMeasure> persistedMeasures = influxDBMapper.query(new Query("SELECT * FROM server_measure",UDP_DATABASE),ServerMeasure.class);
    Assert.assertTrue(persistedMeasures.size()>0);
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

  @Test
  public void testNoDatabaseSpecified() {
    NoDatabaseMeasure noDatabaseMeasure = new NoDatabaseMeasure();
    noDatabaseMeasure.setField(Integer.valueOf(12));
    assertThrows(
        IllegalArgumentException.class,
        () -> influxDBMapper.query(NoDatabaseMeasure.class),
        "Should specify database for this query"
    );
  }

  @Test
  public void testNonInstantTime() {
    NonInstantTime nonInstantTime = new NonInstantTime();
    nonInstantTime.setTime(1234566L);
    assertThrows(
        InfluxDBMapperException.class,
        () -> influxDBMapper.save(nonInstantTime),
        "time should be of type Instant"
    );
  }

  @Test
  public void testInstantOnTime() {
    ServerMeasure serverMeasure = createMeasure();
    Instant instant = Instant.ofEpochMilli(System.currentTimeMillis());
    serverMeasure.setTime(instant);
    influxDBMapper.save(serverMeasure);
    ServerMeasure persistedMeasure = influxDBMapper.query(ServerMeasure.class).get(0);
    Assert.assertEquals(instant,persistedMeasure.getTime());
  }


  @AfterEach
  public void cleanUp() throws Exception {
    influxDB.query(new Query("DROP DATABASE udp", UDP_DATABASE));
  }

  @Measurement(name = "server_measure", database = UDP_DATABASE)
  static class ServerMeasure {

    /** Check the instant conversions */
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

    @Column(name = "memory_utilization")
    private Double memoryUtilization;

    @Column(name = "ip")
    private String ip;

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

    public String getIp() {
      return ip;
    }

    public void setIp(String ip) {
      this.ip = ip;
    }
  }

  @Measurement(name = "invalid_measure", database = UDP_DATABASE)
  static class InvalidMeasure {

    /** Check the instant conversions */
    @Column(name = "illegal_val")
    private BigDecimal val;

    public BigDecimal getVal() {
      return val;
    }

    public void setVal(BigDecimal val) {
      this.val = val;
    }
  }

  @Measurement(name = "no_database_measure")
  static class NoDatabaseMeasure {

    @Column(name = "field")
    private Integer field;

    public Integer getField() {
      return field;
    }

    public void setField(Integer field) {
      this.field = field;
    }
  }

  @Measurement(name = "non_instant_time")
  static class NonInstantTime {

    @Column(name = "time")
    private long time;

    public long getTime() {
      return time;
    }

    public void setTime(long time) {
      this.time = time;
    }
  }

  private ServerMeasure createMeasure() {
    ServerMeasure serverMeasure = new ServerMeasure();
    serverMeasure.setName("maverick");
    serverMeasure.setCpu(4.3d);
    serverMeasure.setHealthy(true);
    serverMeasure.setUptime(1234L);
    serverMeasure.setMemoryUtilization(new Double(34.5));
    serverMeasure.setIp("19.087.4.5");
    return serverMeasure;
  }

}

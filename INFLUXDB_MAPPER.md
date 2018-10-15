### InfluxDBMapper

In case you want to use models only, you can use the InfluxDBMapper to save and load measurements.
You can create models that specify the database the measurement and the retention policy.

```Java
@Measurement(name = "cpu",database="servers", retentionPolicy="autogen",timeUnit = TimeUnit.MILLISECONDS)
public class Cpu {
    @Column(name = "time")
    private Instant time;
    @Column(name = "host", tag = true)
    private String hostname;
    @Column(name = "region", tag = true)
    private String region;
    @Column(name = "idle")
    private Double idle;
    @Column(name = "happydevop")
    private Boolean happydevop;
    @Column(name = "uptimesecs")
    private Long uptimeSecs;
    // getters (and setters if you need)
}
```

Save operation using a model.

```Java
Cpu cpu = .., create the cpu measure
influxDBMapper.save(cpu);
```

Load data using a model.

```java
Cpu persistedCpu = influxDBMapper.query(Cpu.class).get(0);
```

Load data using a query and specify the model for mapping.

```java
Query query = ... create your query
List<Cpu> persistedMeasure = influxDBMapper.query(query,Cpu.class);
```

#### InfluxDBMapper limitations

Tags are automatically converted to strings, since tags are strings to influxdb
Supported values for fields are boolean, int, long, double, Boolean, Integer, Long, Double.
The time field should be of type instant. 
If you do not specify the time or set a value then the current system time shall be used with the timeunit specified.

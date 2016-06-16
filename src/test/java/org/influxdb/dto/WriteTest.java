package org.influxdb.dto;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test for Writing Data
 *
 * @author Daniel700
 */
@Test
public class WriteTest {

    private InfluxDB influxDB;
    private String database = "testDB";

    @BeforeClass
    public void setUp(){
        this.influxDB = InfluxDBFactory.connect("http://" + TestUtils.getInfluxIP() + ":8086", "root", "root");
        influxDB.createDatabase(database);
    }


    @Test
    public void testLineProtocolWithConditionalValues(){
        BatchPoints batchPoints = BatchPoints.database(database).build();
        boolean condition = false;

        for (int i = 0; i < 10; i++){
            if (i%2 == 0) {
                condition = true;
            }
            else
                condition = false;

            Point point = Point.measurement("condEvents")
                    .tag("tag1", "abc")
                    .addField("field1", 1)
                    .addField("field2", 2.5, condition)
                    .time(System.nanoTime(), TimeUnit.NANOSECONDS)
                    .build();

            batchPoints.point(point);
        }
        influxDB.write(batchPoints);

        Query query = new Query("Select Count(field1) from condEvents", database);
        QueryResult result = influxDB.query(query);
        String count = result.getResults().get(0).getSeries().get(0).getValues().get(0).toString();
        String parts[] = count.split(",");
        Query query1 = new Query("Select Count(field2) from condEvents", database);
        QueryResult result1 = influxDB.query(query1);
        String count2 = result1.getResults().get(0).getSeries().get(0).getValues().get(0).toString();
        String parts2[] = count2.split(",");

        Assert.assertEquals(parts[1].trim(),"10.0]");
        Assert.assertEquals(parts2[1].trim(),"5.0]");
    }


    @Test
    public void testValidateBatchProcessor() throws InterruptedException{
        int flushInterval = 800;
        influxDB.enableBatch(25000, flushInterval, TimeUnit.MILLISECONDS);
        int numberOfEvents = 5100000;

        for (int i = 1; i <= numberOfEvents; i++){
            Point point = Point.measurement("batchEvents")
                    .time(i, TimeUnit.NANOSECONDS)
                    .tag("tag1", "abc")
                    .addField("field1", i)
                    .build();

            influxDB.write(database, "default", point);

            if (i%50000 == 0)
                System.out.println(i);
        }

        //Wait for terminating all writes
        Thread.sleep(flushInterval*3);

        Query query = new Query("Select Count(field1) from batchEvents", database);
        QueryResult result = influxDB.query(query);
        String count = result.getResults().get(0).getSeries().get(0).getValues().get(0).toString();
        String parts[] = count.split(",");
        Assert.assertEquals(parts[1].trim(), numberOfEvents + ".0]");
    }


    @Test
    public void testValidateSingleBatchWrite(){

        BatchPoints batchPoints = BatchPoints.database(database).retentionPolicy("default").consistency(InfluxDB.ConsistencyLevel.ALL).build();
        int numberOfEvents = 2100000;

        for (int i = 1; i <= numberOfEvents; i++){
            Point point = Point.measurement("batchEvents")
                    .time(i, TimeUnit.NANOSECONDS)
                    .tag("tag1", "abc")
                    .addField("field1", i)
                    .build();

            batchPoints.point(point);

            if (i%50000 == 0)
                System.out.println(i);
        }

        influxDB.write(batchPoints);

        Query query = new Query("Select Count(field1) from batchEvents", database);
        QueryResult result = influxDB.query(query);
        String count = result.getResults().get(0).getSeries().get(0).getValues().get(0).toString();
        String parts[] = count.split(",");
        Assert.assertEquals(parts[1].trim(), numberOfEvents + ".0]");
    }



    @AfterClass
    public void tearDown(){
        influxDB.deleteDatabase(database);
    }

}

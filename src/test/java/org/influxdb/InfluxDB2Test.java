package org.influxdb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 * @author Jakub Bednar (30/08/2021 11:31)
 */
@RunWith(JUnitPlatform.class)
@EnabledIfEnvironmentVariable(named = "INFLUXDB_VERSION", matches = "2\\.0")
public class InfluxDB2Test {

    private InfluxDB influxDB;

    @BeforeEach
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        String url = String.format("http://%s:%s", TestUtils.getInfluxIP(), TestUtils.getInfluxPORT(true));
        influxDB = InfluxDBFactory
                .connect(url, "my-user", "my-password")
                .setDatabase("mydb")
                .setRetentionPolicy("autogen");
    }

    @AfterEach
    public void cleanup() {
        influxDB.close();
    }

    @Test
    public void testQuery() throws InterruptedException {

        String measurement = TestUtils.getRandomMeasurement();

        // prepare data
        List<String> records = new ArrayList<>();
        records.add(measurement + ",test=a value=1 1");
        records.add(measurement + ",test=a value=2 2");
        influxDB.write(records);

        // query data
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        influxDB.query(new Query("SELECT * FROM " + measurement), 2, queryResult -> countDownLatch.countDown());

        Assertions.assertTrue(countDownLatch.await(2, TimeUnit.SECONDS));
    }
}
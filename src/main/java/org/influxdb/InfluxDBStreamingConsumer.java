package org.influxdb;

import org.influxdb.dto.QueryResult;

/**
 * Consumer that will be invoked when executing streaming queries against InfluxDB.
 *
 * Created by pjmyburg on 2017/02/13.
 */
public interface InfluxDBStreamingConsumer {

    /**
     * Called for every chunked result received from InfluxDB.
     *
     * @param chunk a single QueryResult chunk received from InfluxDB.
     */
    void accept(QueryResult chunk);

    /**
     * Called when the stream is closed.
     */
    void completed();

}

package org.influxdb.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.influxdb.InfluxDB;
import org.influxdb.TestUtils;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;

import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;
import okio.Buffer;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

@RunWith(JUnitPlatform.class)
public class ChunkingExceptionTest {

    @Test
    public void testChunkingIOException() throws IOException, InterruptedException {
        
        testChunkingException(new IOException(), "java.io.IOException");
    }

    @Test
    public void testChunkingEOFException() throws IOException, InterruptedException {

        testChunkingException(new EOFException(), "DONE");
    }

    public void testChunkingException(Exception ex, String message) throws IOException, InterruptedException {

        InfluxDBService influxDBService = mock(InfluxDBService.class);
        JsonAdapter<QueryResult> adapter = mock(JsonAdapter.class);
        Call<ResponseBody> call = mock(Call.class);
        ResponseBody responseBody = mock(ResponseBody.class);

        when(influxDBService.query(any(String.class), any(String.class), any(String.class), any(String.class), anyInt())).thenReturn(call);
        when(responseBody.source()).thenReturn(new Buffer());
        doThrow(ex).when(adapter).fromJson(any(JsonReader.class));

        String url = "http://" + TestUtils.getInfluxIP() + ":" + TestUtils.getInfluxPORT(true);
        InfluxDB influxDB = new InfluxDBImpl(url, "admin", "admin", new OkHttpClient.Builder(), influxDBService, adapter) {
            @Override
            public String version() {
                return "9.99";
            }
        };

        String dbName = "write_unittest_" + System.currentTimeMillis();
        final BlockingQueue<QueryResult> queue = new LinkedBlockingQueue<>();
        Query query = new Query("SELECT * FROM disk", dbName);
        influxDB.query(query, 2, new Consumer<QueryResult>() {
            @Override
            public void accept(QueryResult result) {
                queue.add(result);
            }
        });

        ArgumentCaptor<Callback<ResponseBody>> argumentCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(call).enqueue(argumentCaptor.capture());
        Callback<ResponseBody> callback = argumentCaptor.getValue();

        callback.onResponse(call, Response.success(responseBody));

        QueryResult result = queue.poll(20, TimeUnit.SECONDS);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(message, result.getError());
    }

}

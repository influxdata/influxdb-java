package org.influxdb.impl;

import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import java.io.IOException;

public class RequestAuthenticator implements Authenticator {

    private String userName;
    private String password;

    public RequestAuthenticator(final String userName, final String password) {
        this.userName = userName;
        this.password = password;
    }

    @Override
    public Request authenticate(final Route route, final Response response) throws IOException {
        final int i = 3;
        if (responseCount(response) >= i || userName == null || password == null) {
            return null; // If we've failed 3 times, give up. - in real life, never give up!!
        }
        String credential = Credentials.basic(userName, password);
        return response.request().newBuilder().header("Authorization", credential).build();
    }

    private int responseCount(final Response response) {
        int result = 1;
        Response parentResponse;
        while ((parentResponse = response.priorResponse()) != null) {
            result++;
        }
        return result;
    }
}

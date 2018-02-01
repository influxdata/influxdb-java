package org.influxdb.impl;

import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(JUnitPlatform.class)
public class RequestAuthenticatorTest {

    Response sourceResponse;

    @BeforeEach
    public void setup(){
        Response.Builder responseBuilder = new Response.Builder();
        responseBuilder.request(new Request.Builder().url("http://localhost").build());
        responseBuilder.message("Test");
        responseBuilder.code(2);
        responseBuilder.protocol(Protocol.HTTP_1_1);
        sourceResponse = responseBuilder.build();

    }

    @Test
    public void testCheckAuthenticatorReturnsRequestThatContainsAuthorizationHeader() {

        RequestAuthenticator requestAuthenticator = new RequestAuthenticator("foo","password");
        try {
            Request authenticatedRequest = requestAuthenticator.authenticate(null,sourceResponse);
            Assert.assertTrue(StringUtils.isNotBlank(authenticatedRequest.header("Authorization")));
        } catch (IOException e) {
            Assert.fail("Should not throw an Exception");
        }
    }

    @Test
    public void testCheckAuthenticatorDoesNothingIfNoCredetials() throws IOException {

        RequestAuthenticator requestAuthenticator = new RequestAuthenticator(null,"password");
        Assert.assertNull(requestAuthenticator.authenticate(null,sourceResponse));

        requestAuthenticator = new RequestAuthenticator("foo",null);
        Assert.assertNull(requestAuthenticator.authenticate(null,sourceResponse));

        requestAuthenticator = new RequestAuthenticator(null,null);
        Assert.assertNull(requestAuthenticator.authenticate(null,sourceResponse));
    }
}

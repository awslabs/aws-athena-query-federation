/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.service;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link HttpClientWrapper}.
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpClientWrapperTest {

    @Mock
    private CloseableHttpClient mockHttpClient;

    @Mock
    private CloseableHttpResponse mockResponse;

    private HttpClientWrapper wrapper;

    @Before
    public void setUp() {
        wrapper = new HttpClientWrapper(mockHttpClient);
    }

    @Test
    public void testDefaultConstructor() {
        HttpClientWrapper defaultWrapper = new HttpClientWrapper();
        assertNotNull("Default constructor should create a valid wrapper", defaultWrapper);
    }

    @Test
    public void testConstructorWithClient() {
        HttpClientWrapper customWrapper = new HttpClientWrapper(mockHttpClient);
        assertNotNull("Constructor with client should create a valid wrapper", customWrapper);
    }

    @Test
    public void testExecuteHttpPost() throws IOException {
        HttpPost request = new HttpPost("https://example.com/api");

        when(mockHttpClient.execute(request)).thenReturn(mockResponse);

        CloseableHttpResponse response = wrapper.execute(request);

        assertNotNull("Response should not be null", response);
        assertEquals("Should return the mocked response", mockResponse, response);
        verify(mockHttpClient, times(1)).execute(request);
    }

    @Test
    public void testExecuteHttpGet() throws IOException {
        HttpGet request = new HttpGet("https://example.com/api");

        when(mockHttpClient.execute(request)).thenReturn(mockResponse);

        CloseableHttpResponse response = wrapper.execute(request);

        assertNotNull("Response should not be null", response);
        assertEquals("Should return the mocked response", mockResponse, response);
        verify(mockHttpClient, times(1)).execute(request);
    }

    @Test(expected = IOException.class)
    public void testExecuteHttpPostThrowsIOException() throws IOException {
        HttpPost request = new HttpPost("https://example.com/api");

        when(mockHttpClient.execute(request)).thenThrow(new IOException("Network error"));

        wrapper.execute(request);
    }

    @Test(expected = IOException.class)
    public void testExecuteHttpGetThrowsIOException() throws IOException {
        HttpGet request = new HttpGet("https://example.com/api");

        when(mockHttpClient.execute(request)).thenThrow(new IOException("Network error"));

        wrapper.execute(request);
    }

    @Test
    public void testExecuteMultipleHttpPostRequests() throws IOException {
        HttpPost request1 = new HttpPost("https://example.com/api1");
        HttpPost request2 = new HttpPost("https://example.com/api2");

        when(mockHttpClient.execute(request1)).thenReturn(mockResponse);
        when(mockHttpClient.execute(request2)).thenReturn(mockResponse);

        CloseableHttpResponse response1 = wrapper.execute(request1);
        CloseableHttpResponse response2 = wrapper.execute(request2);

        assertNotNull("First response should not be null", response1);
        assertNotNull("Second response should not be null", response2);
        verify(mockHttpClient, times(1)).execute(request1);
        verify(mockHttpClient, times(1)).execute(request2);
    }

    @Test
    public void testExecuteMultipleHttpGetRequests() throws IOException {
        HttpGet request1 = new HttpGet("https://example.com/api1");
        HttpGet request2 = new HttpGet("https://example.com/api2");

        when(mockHttpClient.execute(request1)).thenReturn(mockResponse);
        when(mockHttpClient.execute(request2)).thenReturn(mockResponse);

        CloseableHttpResponse response1 = wrapper.execute(request1);
        CloseableHttpResponse response2 = wrapper.execute(request2);

        assertNotNull("First response should not be null", response1);
        assertNotNull("Second response should not be null", response2);
        verify(mockHttpClient, times(1)).execute(request1);
        verify(mockHttpClient, times(1)).execute(request2);
    }
}

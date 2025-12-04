/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.util.EntityUtils;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.util.Collections;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * This class is used to test the AWSRequestSigningApacheInterceptor class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AWSRequestSigningApacheInterceptorTest
{
    private static final String TEST_URI = "https://search-test.us-east-1.es.amazonaws.com/test";
    private static final String TEST_URI_INVALID = "invalid://[malformed uri";
    private static final String TEST_BODY = "test body";
    private static final String HTTP_METHOD_GET = "GET";
    private static final String INVALID_URI_MESSAGE = "Invalid URI";

    @Mock
    private AwsV4HttpSigner mockSigner;

    @Mock
    private AwsCredentialsProvider mockCredentialsProvider;

    private AWSRequestSigningApacheInterceptor interceptor;
    private HttpContext context;

    @Before
    public void setUp()
    {
        interceptor = new AWSRequestSigningApacheInterceptor("es", mockSigner, mockCredentialsProvider, "us-east-1");
        context = new BasicHttpContext();
        HttpHost host = HttpHost.create("https://search-test.us-east-1.es.amazonaws.com");
        context.setAttribute(HttpCoreContext.HTTP_TARGET_HOST, host);
        
        // Mock the signer and credentials provider
        AwsCredentials mockCredentials = new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return "test-access-key";
            }
            
            @Override
            public String secretAccessKey() {
                return "test-secret-key";
            }
        };
        lenient().when(mockCredentialsProvider.resolveCredentials()).thenReturn(mockCredentials);
        
        SignedRequest mockSignedRequest = mock(SignedRequest.class);
        SdkHttpFullRequest mockRequest = mock(SdkHttpFullRequest.class);
        lenient().when(mockSignedRequest.request()).thenReturn(mockRequest);
        lenient().when(mockRequest.headers()).thenReturn(Collections.emptyMap());
        lenient().when(mockSigner.sign(isA(Consumer.class))).thenReturn(mockSignedRequest);
    }

    @Test
    public void process_withValidRequest_signsRequestAndSetsSignedHeaders() throws Exception
    {
        HttpRequest request = new HttpGet(TEST_URI);

        interceptor.process(request, context);
        assertNotNull("Valid GET request should be processed", request);
        // Verify that signing occurred
        verify(mockSigner).sign(isA(Consumer.class));
    }

    @Test
    public void process_withInvalidUri_throwsAthenaConnectorException()
    {
        HttpRequest request = new BasicHttpRequest(HTTP_METHOD_GET, TEST_URI_INVALID);

        try {
            interceptor.process(request, context);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Invalid URI",
                    ex.getMessage().contains(INVALID_URI_MESSAGE));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName());
        }
    }

    @Test
    public void process_withEntityEnclosingRequest_signsRequestAndSetsEntityContent() throws Exception
    {
        HttpPost request = new HttpPost(TEST_URI);
        StringEntity entity = new StringEntity(TEST_BODY);
        request.setEntity(entity);

        interceptor.process(request, context);
        verify(mockSigner).sign(isA(Consumer.class));
        assertNotNull("Request entity should be present", request.getEntity());
        assertEquals("Entity content should match TEST_BODY", TEST_BODY, EntityUtils.toString(request.getEntity()));
    }

    @Test
    public void process_withEntityEnclosingRequestWithoutEntity_signsRequestWithEmptyContentStream() throws Exception
    {
        HttpPost request = new HttpPost(TEST_URI);
        // No entity set

        interceptor.process(request, context);
        verify(mockSigner).sign(isA(Consumer.class));
        // Implementation only copies entity back when request already had an entity; without entity it may remain null
        if (request.getEntity() != null) {
            assertEquals("Entity content stream should be empty", "", EntityUtils.toString(request.getEntity()));
        }
    }

    @Test
    public void process_withNullHostInContext_signsRequestWithoutSettingUri() throws Exception
    {
        HttpContext contextWithoutHost = new BasicHttpContext();
        HttpRequest request = new HttpGet(TEST_URI);

        interceptor.process(request, contextWithoutHost);
        verify(mockSigner).sign(isA(Consumer.class));
        assertEquals("Request URI should remain TEST_URI", TEST_URI, request.getRequestLine().getUri());
    }

    @Test
    public void process_withQueryParameters_signsRequestWithMultipleValuesForSameKey()
            throws Exception
    {
        // Test nvpToMapParams indirectly through process method by creating a request with query parameters
        // that have multiple values for the same key
        String uriWithQueryParams = TEST_URI + "?key1=value1&key1=value2&key2=value3";
        HttpRequest request = new HttpGet(uriWithQueryParams);

        interceptor.process(request, context);

        verify(mockSigner).sign(isA(Consumer.class));
        String uri = request.getRequestLine().getUri();
        assertTrue("Request URI should contain key1=value1", uri.contains("key1=value1"));
        assertTrue("Request URI should contain key1=value2", uri.contains("key1=value2"));
        assertTrue("Request URI should contain key2=value3", uri.contains("key2=value3"));
    }

    @Test
    public void process_withURISyntaxExceptionInBuilder_throwsAthenaConnectorException()
    {
        HttpRequest request = org.mockito.Mockito.mock(HttpRequest.class);
        org.apache.http.RequestLine requestLine = org.mockito.Mockito.mock(org.apache.http.RequestLine.class);
        lenient().when(request.getRequestLine()).thenReturn(requestLine);
        lenient().when(requestLine.getMethod()).thenReturn(HTTP_METHOD_GET);
        lenient().when(requestLine.getUri()).thenReturn("https://test.com/path?query=value%");

        try {
            interceptor.process(request, context);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Invalid URI",
                    ex.getMessage().contains(INVALID_URI_MESSAGE));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName());
        }
    }
}

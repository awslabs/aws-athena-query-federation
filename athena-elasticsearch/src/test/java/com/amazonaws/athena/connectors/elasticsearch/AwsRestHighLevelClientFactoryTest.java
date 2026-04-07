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

import org.elasticsearch.client.RequestOptions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the AwsRestHighLevelClientFactory class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AwsRestHighLevelClientFactoryTest
{
    private static final String TEST_ENDPOINT = "https://search-test.us-east-1.es.amazonaws.com";
    private static final String TEST_USERNAME = "testuser";
    private static final String TEST_PASSWORD = "testpass";
    private static final String TEST_ENDPOINT_WITH_CREDENTIALS = "https://" + TEST_USERNAME + "@" + TEST_PASSWORD
            + ":www.example.com";

    @Mock
    private AwsRestHighLevelClient mockClient;

    private AwsRestHighLevelClientFactory factoryWithAwsCredentials;
    private AwsRestHighLevelClientFactory factoryWithoutAwsCredentials;

    @Before
    public void setUp()
    {
        factoryWithAwsCredentials = new AwsRestHighLevelClientFactory(true);
        factoryWithoutAwsCredentials = new AwsRestHighLevelClientFactory(false);
    }

    @Test
    public void getOrCreateClient_withAwsCredentials_returnsNonNullClient()
            throws IOException
    {
        try (MockedConstruction<AwsRestHighLevelClient.Builder> construction = mockConstruction(AwsRestHighLevelClient.Builder.class,
                (mock, context) -> {
                    when(mock.withCredentials(isA(AwsCredentialsProvider.class))).thenReturn(mock);
                    when(mock.build()).thenReturn(mockClient);
                })) {
            when(mockClient.ping(eq(RequestOptions.DEFAULT))).thenReturn(true);
            AwsRestHighLevelClient client1 = factoryWithAwsCredentials.getOrCreateClient(TEST_ENDPOINT);
            AwsRestHighLevelClient client2 = factoryWithAwsCredentials.getOrCreateClient(TEST_ENDPOINT);

            verify(construction.constructed().get(0)).withCredentials(isA(AwsCredentialsProvider.class));
            verify(construction.constructed().get(0)).build();
            assertNotNull("Client with AWS credentials should not be null", client1);
            assertSame("Second call should return cached client", client1, client2);
            assertEquals("Builder should be constructed only once due to cache", 1, construction.constructed().size());
        }
    }

    @Test
    public void getOrCreateClient_withoutAwsCredentials_returnsBuiltClient()
            throws IOException
    {
        try (MockedConstruction<AwsRestHighLevelClient.Builder> construction = mockConstruction(AwsRestHighLevelClient.Builder.class,
                (mock, context) -> when(mock.build()).thenReturn(mockClient))) {
            when(mockClient.ping(eq(RequestOptions.DEFAULT))).thenReturn(true);
            AwsRestHighLevelClient client1 = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT);
            AwsRestHighLevelClient client2 = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT);

            verify(construction.constructed().get(0)).build();
            assertNotNull("Client without AWS credentials should not be null", client1);
            assertSame("Second call should return cached client", client1, client2);
            assertEquals("Builder should be constructed only once due to cache", 1, construction.constructed().size());
        }
    }

    @Test
    public void getOrCreateClient_withEmbeddedCredentials_returnsClientWithCredentials()
            throws IOException
    {
        try (MockedConstruction<AwsRestHighLevelClient.Builder> construction = mockConstruction(AwsRestHighLevelClient.Builder.class,
                (mock, context) -> {
                    when(mock.withCredentials(TEST_USERNAME, TEST_PASSWORD)).thenReturn(mock);
                    when(mock.build()).thenReturn(mockClient);
                })) {
            when(mockClient.ping(eq(RequestOptions.DEFAULT))).thenReturn(true);
            AwsRestHighLevelClient client1 = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT_WITH_CREDENTIALS);
            AwsRestHighLevelClient client2 = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT_WITH_CREDENTIALS);

            verify(construction.constructed().get(0)).withCredentials(TEST_USERNAME, TEST_PASSWORD);
            verify(construction.constructed().get(0)).build();
            assertNotNull("Client with embedded credentials should not be null", client1);
            assertSame("Second call should return cached client", client1, client2);
            assertEquals("Builder should be constructed only once due to cache", 1, construction.constructed().size());
        }
    }

    @Test
    public void getOrCreateClient_withUsernameAndPassword_returnsClientWithBasicAuth()
            throws IOException
    {
        try (MockedConstruction<AwsRestHighLevelClient.Builder> construction = mockConstruction(AwsRestHighLevelClient.Builder.class,
                (mock, context) -> {
                    when(mock.withCredentials(TEST_USERNAME, TEST_PASSWORD)).thenReturn(mock);
                    when(mock.build()).thenReturn(mockClient);
                })) {
            when(mockClient.ping(eq(RequestOptions.DEFAULT))).thenReturn(true);
            AwsRestHighLevelClient client1 = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT, TEST_USERNAME, TEST_PASSWORD);
            AwsRestHighLevelClient client2 = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT, TEST_USERNAME, TEST_PASSWORD);

            verify(construction.constructed().get(0)).withCredentials(TEST_USERNAME, TEST_PASSWORD);
            verify(construction.constructed().get(0)).build();
            assertNotNull("Client with username and password should not be null", client1);
            assertSame("Second call should return cached client", client1, client2);
            assertEquals("Builder should be constructed only once due to cache", 1, construction.constructed().size());
        }
    }
}

/*-
 * #%L
 * athena-mongodb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.docdb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.MockedStatic;

public class DocDBConnectionFactoryTest
{
    private static final String TRUST_STORE_TYPE_PROPERTY = "javax.net.ssl.trustStoreType";
    private static final String TRUST_STORE_PASSWORD_PROPERTY = "javax.net.ssl.trustStorePassword";
    private static final String DOCDB_CONN_STR = "mongodb://localhost:27017";
    
    private DocDBConnectionFactory connectionFactory;

    @Before
    public void setUp()
            throws Exception
    {
        connectionFactory = new DocDBConnectionFactory();
    }

    @Test
    public void clientCacheHitTest()
            throws IOException
    {
        MongoClient mockConn = mock(MongoClient.class);
        when(mockConn.listDatabaseNames()).thenReturn(null);

        connectionFactory.addConnection("conStr", mockConn);
        MongoClient conn = connectionFactory.getOrCreateConn("conStr");

        assertEquals(mockConn, conn);
        verify(mockConn, times(1)).listDatabaseNames();
    }

    @Test
    public void getOrCreateConn_withSSLConnection_setsTrustStoreProperties()
    {
        String originalTrustStoreType = System.getProperty(TRUST_STORE_TYPE_PROPERTY);
        String originalTrustStorePassword = System.getProperty(TRUST_STORE_PASSWORD_PROPERTY);

        try {
            System.clearProperty(TRUST_STORE_TYPE_PROPERTY);
            System.clearProperty(TRUST_STORE_PASSWORD_PROPERTY);

            try (MockedStatic<MongoClients> mockedStatic = mockStatic(MongoClients.class)) {
                MongoClient mockClient = mock(MongoClient.class);
                mockedStatic.when(() -> MongoClients.create(anyString())).thenReturn(mockClient);

                connectionFactory.getOrCreateConn("mongodb://localhost:27017/?ssl=true");
            }

            assertEquals("JKS", System.getProperty(TRUST_STORE_TYPE_PROPERTY));
            assertEquals("changeit", System.getProperty(TRUST_STORE_PASSWORD_PROPERTY));
        }
        finally {
            // Restore original system properties
            if (originalTrustStoreType != null) {
                System.setProperty(TRUST_STORE_TYPE_PROPERTY, originalTrustStoreType);
            }
            else {
                System.clearProperty(TRUST_STORE_TYPE_PROPERTY);
            }
            if (originalTrustStorePassword != null) {
                System.setProperty(TRUST_STORE_PASSWORD_PROPERTY, originalTrustStorePassword);
            }
            else {
                System.clearProperty(TRUST_STORE_PASSWORD_PROPERTY);
            }
        }
    }

    @Test
    public void getOrCreateConn_withConnectionFailure_createsNewConnection()
    {
        MongoClient mockConn = mock(MongoClient.class);
        when(mockConn.listDatabaseNames()).thenThrow(new RuntimeException("Test exception"));

        MongoClient newConnMock = mock(MongoClient.class);

        try (MockedStatic<MongoClients> mockedStatic = mockStatic(MongoClients.class)) {
            mockedStatic.when(() -> MongoClients.create(anyString())).thenReturn(newConnMock);

            connectionFactory.addConnection(DOCDB_CONN_STR, mockConn);
            MongoClient result = connectionFactory.getOrCreateConn(DOCDB_CONN_STR);

            try {
                // Verify that the original connection was tested
                verify(mockConn, times(1)).listDatabaseNames();
                // Verify that we got a new connection (not our mock) since the test failed
                assertNotNull("Result should not be null", result);
                assertNotEquals("Should create new connection when test fails", mockConn, result);
            }
            finally {
                // Clean up the created connection
                if (result != null && result != mockConn) {
                    result.close();
                }
            }
        }
    }
}

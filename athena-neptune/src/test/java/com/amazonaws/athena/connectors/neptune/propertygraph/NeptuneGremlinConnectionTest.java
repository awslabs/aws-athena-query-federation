/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune.propertygraph;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertNotNull;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NeptuneGremlinConnectionTest {

    private static final String TEST_ENDPOINT = "test-endpoint";
    private static final String TEST_PORT = "8182";
    private static final String TEST_REGION = "us-east-1";
    public static final int PORT = 8182;

    private NeptuneGremlinConnection connection;
    private Cluster mockCluster;
    private Client mockClient;

    @Before
    public void setUp() {
        mockCluster = mock(Cluster.class);
        mockClient = mock(Client.class);
    }

    @After
    public void tearDown() {
        if (connection != null) {
            connection.closeCluster();
        }
    }

    @Test
    public void constructor_withoutIam_createsConnectionWithCorrectConfiguration() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class)) {
            Cluster.Builder mockBuilder = mockClusterBuilderChain(mockedCluster);
            when(mockBuilder.create()).thenReturn(mockCluster);
            
            // Create connection
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);


            verify(mockBuilder, times(2)).addContactPoint(TEST_ENDPOINT);
            verify(mockBuilder, times(2)).port(PORT);
            verify(mockBuilder, times(2)).enableSsl(true);
            verify(mockBuilder, times(2)).create();
        }
    }

    @Test
    public void constructor_withIam_createsConnectionWithHandshakeInterceptor() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class)) {
            Cluster.Builder mockBuilder = mockClusterBuilderChain(mockedCluster);
            when(mockBuilder.handshakeInterceptor(any())).thenReturn(mockBuilder);
            when(mockBuilder.create()).thenReturn(mockCluster);
            
            // Create connection
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, true, TEST_REGION);
            
            verify(mockBuilder, times(2)).addContactPoint(TEST_ENDPOINT);
            verify(mockBuilder, times(2)).port(PORT);
            verify(mockBuilder, times(2)).enableSsl(true);
            verify(mockBuilder, times(2)).handshakeInterceptor(any());
            verify(mockBuilder, times(2)).create();
        }
    }

    @Test
    public void getNeptuneClientConnection_withValidConnection_returnsClient() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class)) {

            Cluster.Builder mockBuilder = mockClusterBuilderChain(mockedCluster);

            when(mockBuilder.create()).thenReturn(mockCluster);

            // Mock cluster.connect()
            when(mockCluster.connect()).thenReturn(mockClient);

            // Create connection and get client
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
            Client client = connection.getNeptuneClientConnection();

            // Verify
            assertNotNull(client);
            verify(mockCluster).connect();
        }
    }

    @Test
    public void getTraversalSource_withValidClient_returnsTraversalSource() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class);
             MockedStatic<DriverRemoteConnection> mockedConnection = mockStatic(DriverRemoteConnection.class)) {

            Cluster.Builder mockBuilder = mockClusterBuilderChain(mockedCluster);
            when(mockBuilder.create()).thenReturn(mockCluster);

            // Mock DriverRemoteConnection
            DriverRemoteConnection mockRemoteConnection = mock(DriverRemoteConnection.class);
            mockedConnection.when(() -> DriverRemoteConnection.using(mockClient))
                    .thenReturn(mockRemoteConnection);


            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
            GraphTraversalSource traversalSource = connection.getTraversalSource(mockClient);

            // Verify
            assertNotNull(traversalSource);
            mockedConnection.verify(() -> DriverRemoteConnection.using(mockClient));
        }
    }

    @Test
    public void closeCluster_withValidConnection_closesCluster() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class)) {
            Cluster.Builder mockBuilder = mockClusterBuilderChain(mockedCluster);
            when(mockBuilder.create()).thenReturn(mockCluster);

            // Create connection and close cluster
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
            connection.closeCluster();

            // Verify
            verify(mockCluster).close();
        }
    }

    private Cluster.Builder mockClusterBuilderChain(MockedStatic<Cluster> mockedCluster) {
        Cluster.Builder mockBuilder = mock(Cluster.Builder.class);
        mockedCluster.when(Cluster::build).thenReturn(mockBuilder);
        when(mockBuilder.addContactPoint(TEST_ENDPOINT)).thenReturn(mockBuilder);
        when(mockBuilder.port(PORT)).thenReturn(mockBuilder);
        when(mockBuilder.enableSsl(true)).thenReturn(mockBuilder);
        return mockBuilder;
    }
} 
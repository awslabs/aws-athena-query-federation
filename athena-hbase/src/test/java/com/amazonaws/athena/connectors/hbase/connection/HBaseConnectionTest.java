/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase.connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HBaseConnectionTest
{
    private static final Logger logger = LoggerFactory.getLogger(HBaseConnectionTest.class);

    private int maxRetries = 3;
    private Connection mockConnection;
    private Admin mockAdmin;
    private Table mockTable;
    private ResultScanner mockScanner;
    private HBaseConnection connection;

    @Before
    public void setUp()
            throws Exception
    {
        mockConnection = mock(Connection.class);
        mockAdmin = mock(Admin.class);
        mockTable = mock(Table.class);
        mockScanner = mock(ResultScanner.class);
        connection = new HBaseConnectionStub(mockConnection, maxRetries);
    }

    @Test
    public void listNamespaceDescriptors()
            throws IOException
    {
        logger.info("listNamespaceDescriptors: enter");
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});

        NamespaceDescriptor[] result = connection.listNamespaceDescriptors();
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).listNamespaceDescriptors();
        logger.info("listNamespaceDescriptors: exit");
    }

    @Test
    public void listNamespaceDescriptorsWithRetry()
            throws IOException
    {
        logger.info("listNamespaceDescriptorsWithRetry: enter");
        when(mockConnection.getAdmin()).thenAnswer(new Answer()
        {
            private int count = 0;

            public Object answer(InvocationOnMock invocation)
            {
                if (++count == 1) {
                    //first invocation should throw
                    return new RuntimeException("Retryable");
                }

                return mockAdmin;
            }
        });
        when(mockAdmin.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});

        NamespaceDescriptor[] result = connection.listNamespaceDescriptors();
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(1, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).listNamespaceDescriptors();
        logger.info("listNamespaceDescriptorsWithRetry: exit");
    }

    @Test
    public void listNamespaceDescriptorsRetryExhausted()
            throws IOException
    {
        logger.info("listNamespaceDescriptorsRetryExhausted: enter");
        when(mockConnection.getAdmin()).thenThrow(new RuntimeException("Retryable"));
        try {
            connection.listNamespaceDescriptors();
            fail("Should not reach this line because retries should be exhausted.");
        }
        catch (RuntimeException ex) {
            logger.info("listNamespaceDescriptorsRetryExhausted: Encountered expected exception.", ex);
        }
        assertFalse(connection.isHealthy());
        assertEquals(3, connection.getRetries());
        logger.info("listNamespaceDescriptorsRetryExhausted: exit");
    }

    @Test
    public void listTableNamesByNamespace()
            throws IOException
    {
        logger.info("listTableNamesByNamespace: enter");
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.listTableNamesByNamespace(nullable(String.class))).thenReturn(new TableName[] {});

        TableName[] result = connection.listTableNamesByNamespace("schemaName");
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).listTableNamesByNamespace(any());
        logger.info("listTableNamesByNamespace: exit");
    }

    @Test
    public void listTableNamesByNamespaceWithRetry()
            throws IOException
    {
        logger.info("listTableNamesByNamespaceWithRetry: enter");
        when(mockConnection.getAdmin()).thenAnswer(new Answer()
        {
            private int count = 0;

            public Object answer(InvocationOnMock invocation)
            {
                if (++count == 1) {
                    //first invocation should throw
                    return new RuntimeException("Retryable");
                }

                return mockAdmin;
            }
        });
        when(mockAdmin.listTableNamesByNamespace(nullable(String.class))).thenReturn(new TableName[] {});

        TableName[] result = connection.listTableNamesByNamespace("schemaName");
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(1, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).listTableNamesByNamespace(any());
        logger.info("listTableNamesByNamespaceWithRetry: exit");
    }

    @Test
    public void listTableNamesByNamespaceRetryExhausted()
            throws IOException
    {
        logger.info("listTableNamesByNamespaceRetryExhausted: enter");
        when(mockConnection.getAdmin()).thenThrow(new RuntimeException("Retryable"));
        try {
            connection.listTableNamesByNamespace("schemaName");
            fail("Should not reach this line because retries should be exhausted.");
        }
        catch (RuntimeException ex) {
            logger.info("listTableNamesByNamespaceRetryExhausted: Encountered expected exception.", ex);
        }
        assertFalse(connection.isHealthy());
        assertEquals(3, connection.getRetries());
        logger.info("listTableNamesByNamespaceRetryExhausted: exit");
    }

    @Test
    public void getTableRegions()
            throws IOException
    {
        logger.info("getTableRegions: enter");
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.getTableRegions(any())).thenReturn(new ArrayList<>());

        List<HRegionInfo> result = connection.getTableRegions(null);
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).getTableRegions(any());
        logger.info("getTableRegions: exit");
    }

    @Test
    public void getTableRegionsWithRetry()
            throws IOException
    {
        logger.info("getTableRegionsWithRetry: enter");
        when(mockConnection.getAdmin()).thenAnswer(new Answer()
        {
            private int count = 0;

            public Object answer(InvocationOnMock invocation)
            {
                if (++count == 1) {
                    //first invocation should throw
                    return new RuntimeException("Retryable");
                }

                return mockAdmin;
            }
        });
        when(mockAdmin.getTableRegions(any())).thenReturn(new ArrayList<>());

        List<HRegionInfo> result = connection.getTableRegions(null);
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(1, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).getTableRegions(any());
        logger.info("getTableRegionsWithRetry: exit");
    }

    @Test
    public void getTableRegionsRetryExhausted()
            throws IOException
    {
        logger.info("getTableRegionsRetryExhausted: enter");
        when(mockConnection.getAdmin()).thenThrow(new RuntimeException("Retryable"));
        try {
            connection.getTableRegions(null);
            fail("Should not reach this line because retries should be exhausted.");
        }
        catch (RuntimeException ex) {
            logger.info("listTableNamesByNamespaceRetryExhausted: Encountered expected exception.", ex);
        }
        assertFalse(connection.isHealthy());
        assertEquals(3, connection.getRetries());
        logger.info("getTableRegionsRetryExhausted: exit");
    }

    @Test
    public void scanTable()
            throws IOException
    {
        logger.info("scanTable: enter");
        when(mockConnection.getTable(nullable(org.apache.hadoop.hbase.TableName.class))).thenReturn(mockTable);
        when(mockTable.getScanner(nullable(Scan.class))).thenReturn(mockScanner);

        TableName tableName = org.apache.hadoop.hbase.TableName.valueOf("schema1", "table1");
        boolean result = connection.scanTable(tableName, mock(Scan.class), (ResultScanner scanner) -> scanner != null);

        assertTrue(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getTable(any());
        verify(mockTable, atLeastOnce()).getScanner(nullable(Scan.class));
        logger.info("scanTable: exit");
    }

    @Test
    public void scanTableWithRetry()
            throws IOException
    {
        logger.info("scanTableWithRetry: enter");
        when(mockConnection.getTable(nullable(org.apache.hadoop.hbase.TableName.class))).thenReturn(mockTable);
        when(mockTable.getScanner(nullable(Scan.class))).thenAnswer(new Answer()
        {
            private int count = 0;

            public Object answer(InvocationOnMock invocation)
            {
                if (++count == 1) {
                    //first invocation should throw
                    return new RuntimeException("Retryable");
                }

                return mockScanner;
            }
        });

        TableName tableName = org.apache.hadoop.hbase.TableName.valueOf("schema1", "table1");
        boolean result = connection.scanTable(tableName, mock(Scan.class), (ResultScanner scanner) -> scanner != null);

        assertTrue(result);
        assertTrue(connection.isHealthy());
        assertEquals(1, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getTable(any());
        verify(mockTable, atLeastOnce()).getScanner(nullable(Scan.class));
        logger.info("scanTableWithRetry: exit");
    }

    @Test
    public void scanTableRetriesExhausted()
            throws IOException
    {
        logger.info("scanTableRetriesExhausted: enter");
        when(mockConnection.getTable(nullable(org.apache.hadoop.hbase.TableName.class))).thenReturn(mockTable);
        when(mockTable.getScanner(nullable(Scan.class))).thenThrow(new RuntimeException("Retryable"));
        TableName tableName = org.apache.hadoop.hbase.TableName.valueOf("schema1", "table1");

        try {
            connection.scanTable(tableName, mock(Scan.class), (ResultScanner scanner) -> scanner != null);
        }
        catch (RuntimeException ex) {
            logger.info("listTableNamesByNamespaceRetryExhausted: Encountered expected exception.", ex);
        }

        assertFalse(connection.isHealthy());
        assertEquals(3, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getTable(any());
        verify(mockTable, atLeastOnce()).getScanner(nullable(Scan.class));
        logger.info("scanTableRetriesExhausted: exit");
    }

    @Test
    public void scanTableWithCallerException()
            throws IOException
    {
        logger.info("scanTable: enter");
        when(mockConnection.getTable(nullable(org.apache.hadoop.hbase.TableName.class))).thenReturn(mockTable);
        when(mockTable.getScanner(nullable(Scan.class))).thenReturn(mockScanner);

        TableName tableName = org.apache.hadoop.hbase.TableName.valueOf("schema1", "table1");
        try {
            connection.scanTable(tableName, mock(Scan.class), (ResultScanner scanner) -> {
                throw new RuntimeException("Do not retry!");
            });
        }
        catch (RuntimeException ex) {
            assertTrue(UnrecoverableException.class.equals(ex.getCause().getClass()));
            logger.info("listTableNamesByNamespaceRetryExhausted: Encountered expected exception.", ex);
        }

        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getTable(any());
        verify(mockTable, atLeastOnce()).getScanner(nullable(Scan.class));
        logger.info("scanTable: exit");
    }

    @Test
    public void close()
            throws IOException
    {
        connection.close();
        verify(mockConnection, times(1)).close();
    }

    /**
     * This stub is used to intercept and reroute calls to connect to HBase itself.
     */
    public class HBaseConnectionStub
            extends HBaseConnection
    {
        public HBaseConnectionStub(Connection connection, int maxRetries)
        {
            super(connection, maxRetries);
        }

        @Override
        protected synchronized Connection connect(Configuration config)
        {
            return mockConnection;
        }
    }
}

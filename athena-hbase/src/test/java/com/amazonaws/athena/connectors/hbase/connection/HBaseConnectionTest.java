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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
    public void setUp() {
        mockConnection = mock(Connection.class);
        mockAdmin = mock(Admin.class);
        mockTable = mock(Table.class);
        mockScanner = mock(ResultScanner.class);
        connection = new HBaseConnectionStub(mockConnection, maxRetries);
    }

    @Test
    public void listNamespaceDescriptors_whenAdminReturnsEmpty_returnsEmptyArrayAndStaysHealthy()
            throws IOException
    {
        logger.info("listNamespaceDescriptors_whenAdminReturnsEmpty_returnsEmptyArrayAndStaysHealthy: enter");
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});

        NamespaceDescriptor[] result = connection.listNamespaceDescriptors();
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).listNamespaceDescriptors();
        logger.info("listNamespaceDescriptors_whenAdminReturnsEmpty_returnsEmptyArrayAndStaysHealthy: exit");
    }

    @Test
    public void listNamespaceDescriptors_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsDescriptors()
            throws IOException
    {
        logger.info("listNamespaceDescriptors_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsDescriptors: enter");
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
        logger.info("listNamespaceDescriptors_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsDescriptors: exit");
    }

    @Test
    public void listNamespaceDescriptors_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy()
            throws IOException
    {
        logger.info("listNamespaceDescriptors_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy: enter");
        when(mockConnection.getAdmin()).thenThrow(new RuntimeException("Retryable"));

        assertThrows(RuntimeException.class, () -> connection.listNamespaceDescriptors());
        assertFalse(connection.isHealthy());
        assertEquals(3, connection.getRetries());
        logger.info("listNamespaceDescriptors_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy: exit");
    }

    @Test
    public void listTableNamesByNamespace_whenAdminReturnsEmpty_returnsEmptyArrayAndStaysHealthy()
            throws IOException
    {
        logger.info("listTableNamesByNamespace_whenAdminReturnsEmpty_returnsEmptyArrayAndStaysHealthy: enter");
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.listTableNamesByNamespace(nullable(String.class))).thenReturn(new TableName[] {});

        TableName[] result = connection.listTableNamesByNamespace("schemaName");
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).listTableNamesByNamespace(any());
        logger.info("listTableNamesByNamespace_whenAdminReturnsEmpty_returnsEmptyArrayAndStaysHealthy: exit");
    }

    @Test
    public void listTableNamesByNamespace_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsResult()
            throws IOException
    {
        logger.info("listTableNamesByNamespace_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsResult: enter");
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
        logger.info("listTableNamesByNamespace_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsResult: exit");
    }

    @Test
    public void listTableNamesByNamespace_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy()
            throws IOException
    {
        logger.info("listTableNamesByNamespace_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy: enter");
        when(mockConnection.getAdmin()).thenThrow(new RuntimeException("Retryable"));

        assertThrows(RuntimeException.class, () -> connection.listTableNamesByNamespace("schemaName"));
        assertFalse("Connection should be unhealthy after exhausted retries", connection.isHealthy());
        assertEquals("Retry count should be 3 after exhaustion", 3, connection.getRetries());
        logger.info("listTableNamesByNamespace_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy: exit");
    }

    @Test
    public void getTableRegions_whenAdminReturnsEmptyList_returnsEmptyListAndStaysHealthy()
            throws IOException
    {
        logger.info("getTableRegions_whenAdminReturnsEmptyList_returnsEmptyListAndStaysHealthy: enter");
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.getTableRegions(any())).thenReturn(new ArrayList<>());

        List<HRegionInfo> result = connection.getTableRegions(null);
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).getTableRegions(any());
        logger.info("getTableRegions_whenAdminReturnsEmptyList_returnsEmptyListAndStaysHealthy: exit");
    }

    @Test
    public void getTableRegions_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsResult()
            throws IOException
    {
        logger.info("getTableRegions_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsResult: enter");
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
        logger.info("getTableRegions_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsResult: exit");
    }

    @Test
    public void getTableRegions_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy()
            throws IOException
    {
        logger.info("getTableRegions_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy: enter");
        when(mockConnection.getAdmin()).thenThrow(new RuntimeException("Retryable"));

        assertThrows(RuntimeException.class, () -> connection.getTableRegions(null));
        assertFalse(connection.isHealthy());
        assertEquals(3, connection.getRetries());
        logger.info("getTableRegions_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy: exit");
    }

    @Test
    public void scanTable_whenTableAndScannerSucceed_returnsTrueAndStaysHealthy()
            throws IOException
    {
        logger.info("scanTable_whenTableAndScannerSucceed_returnsTrueAndStaysHealthy: enter");
        when(mockConnection.getTable(nullable(org.apache.hadoop.hbase.TableName.class))).thenReturn(mockTable);
        when(mockTable.getScanner(nullable(Scan.class))).thenReturn(mockScanner);

        TableName tableName = org.apache.hadoop.hbase.TableName.valueOf("schema1", "table1");
        boolean result = connection.scanTable(tableName, mock(Scan.class), (ResultScanner scanner) -> scanner != null);

        assertTrue(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getTable(any());
        verify(mockTable, atLeastOnce()).getScanner(nullable(Scan.class));
        logger.info("scanTable_whenTableAndScannerSucceed_returnsTrueAndStaysHealthy: exit");
    }

    @Test
    public void scanTable_whenFirstGetScannerThrowsRuntimeException_retriesAndReturnsTrue()
            throws IOException
    {
        logger.info("scanTable_whenFirstGetScannerThrowsRuntimeException_retriesAndReturnsTrue: enter");
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
        logger.info("scanTable_whenFirstGetScannerThrowsRuntimeException_retriesAndReturnsTrue: exit");
    }

    @Test
    public void scanTable_whenGetScannerAlwaysThrows_throwsRuntimeExceptionAndMarksUnhealthy()
            throws IOException
    {
        logger.info("scanTable_whenGetScannerAlwaysThrows_throwsRuntimeExceptionAndMarksUnhealthy: enter");
        when(mockConnection.getTable(nullable(org.apache.hadoop.hbase.TableName.class))).thenReturn(mockTable);
        when(mockTable.getScanner(nullable(Scan.class))).thenThrow(new RuntimeException("Retryable"));
        TableName tableName = org.apache.hadoop.hbase.TableName.valueOf("schema1", "table1");

        assertThrows(RuntimeException.class, () ->
                connection.scanTable(tableName, mock(Scan.class), (ResultScanner scanner) -> scanner != null));

        assertFalse(connection.isHealthy());
        assertEquals(3, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getTable(any());
        verify(mockTable, atLeastOnce()).getScanner(nullable(Scan.class));
        logger.info("scanTable_whenGetScannerAlwaysThrows_throwsRuntimeExceptionAndMarksUnhealthy: exit");
    }

    @Test
    public void scanTable_whenResultProcessorThrowsUnrecoverableException_doesNotRetryAndStaysHealthy()
            throws IOException
    {
        logger.info("scanTable_whenResultProcessorThrowsUnrecoverableException_doesNotRetryAndStaysHealthy: enter");
        when(mockConnection.getTable(nullable(org.apache.hadoop.hbase.TableName.class))).thenReturn(mockTable);
        when(mockTable.getScanner(nullable(Scan.class))).thenReturn(mockScanner);

        TableName tableName = org.apache.hadoop.hbase.TableName.valueOf("schema1", "table1");

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                connection.scanTable(tableName, mock(Scan.class), (ResultScanner scanner) -> {
                    throw new RuntimeException("Do not retry!");
                }));
        assertEquals(UnrecoverableException.class, ex.getCause().getClass());

        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getTable(any());
        verify(mockTable, atLeastOnce()).getScanner(nullable(Scan.class));
        logger.info("scanTable_whenResultProcessorThrowsUnrecoverableException_doesNotRetryAndStaysHealthy: exit");
    }

    @Test
    public void tableExists_whenTablePresent_returnsTrueAndStaysHealthy()
            throws IOException
    {
        logger.info("tableExists_whenTablePresent_returnsTrueAndStaysHealthy: enter");
        when(mockConnection.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.tableExists(any())).thenReturn(true);

        boolean result = connection.tableExists(null);
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(0, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).tableExists(any());
        logger.info("tableExists_whenTablePresent_returnsTrueAndStaysHealthy: exit");
    }

    @Test
    public void tableExists_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsFalse()
            throws IOException
    {
        logger.info("tableExists_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsFalse: enter");
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
        when(mockAdmin.tableExists(any())).thenReturn(false);

        boolean result = connection.tableExists(null);
        assertNotNull(result);
        assertTrue(connection.isHealthy());
        assertEquals(1, connection.getRetries());
        verify(mockConnection, atLeastOnce()).getAdmin();
        verify(mockAdmin, atLeastOnce()).tableExists(any());
        logger.info("tableExists_whenFirstGetAdminThrowsRuntimeException_retriesAndReturnsFalse: exit");
    }

    @Test
    public void tableExists_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy()
            throws IOException
    {
        logger.info("tableExists_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy: enter");
        when(mockConnection.getAdmin()).thenThrow(new RuntimeException("Retryable"));

        assertThrows(RuntimeException.class, () -> connection.tableExists(null));
        assertFalse("Connection should be unhealthy after exhausted retries", connection.isHealthy());
        assertEquals("Retry count should be 3 after exhaustion", 3, connection.getRetries());
        logger.info("tableExists_whenRetriesExhausted_throwsRuntimeExceptionAndMarksUnhealthy: exit");
    }

    @Test
    public void close_whenConnectionSet_closesUnderlyingConnection()
            throws IOException
    {
        connection.close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void close_whenIOException_doesNotThrowIOException()
            throws IOException
    {
        Mockito.doThrow(new IOException("Close failed")).when(mockConnection).close();
        connection.close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void close_whenRuntimeException_doesNotThrowRuntimeException()
            throws IOException
    {
        Mockito.doThrow(new RuntimeException("Close failed")).when(mockConnection).close();
        connection.close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void close_whenNullConnection_doesNotThrowException()
    {
        HBaseConnection nullConn = new HBaseConnectionStub(null, maxRetries);
        nullConn.close();
    }

    @Test
    public void listNamespaceDescriptors_whenInterruptedException_throwsRuntimeException()
            throws IOException
    {
        logger.info("listNamespaceDescriptors_whenInterruptedException_throwsRuntimeException: enter");
        when(mockConnection.getAdmin()).thenAnswer((Answer) invocation -> {
            throw new InterruptedException("Interrupted");
        });

        RuntimeException ex = assertThrows(RuntimeException.class, () -> connection.listNamespaceDescriptors());
        assertTrue("Exception should be caused by InterruptedException or contain interruption message",
                (ex.getCause() != null && ex.getCause() instanceof InterruptedException) ||
                        (ex.getMessage() != null && ex.getMessage().contains("Interrupted")));
        logger.info("listNamespaceDescriptors_whenInterruptedException_throwsRuntimeException: exit");
    }

    @Test
    public void listNamespaceDescriptors_whenConnectionNull_throwsRuntimeException()
    {
        HBaseConnection nullConn = new HBaseConnectionStub(null, maxRetries);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> nullConn.listNamespaceDescriptors());
        assertTrue("Exception should indicate invalid connection or exhausted retries",
                ex.getMessage() != null && (ex.getMessage().contains("Invalid connection") ||
                        ex.getMessage().contains("Exhausted hbase retries")));
    }

    @Test
    public void listNamespaceDescriptors_whenReconnectThrows_marksConnectionUnhealthyAndThrowsRuntimeException()
            throws IOException
    {
        logger.info("listNamespaceDescriptors_whenReconnectThrows_marksConnectionUnhealthyAndThrowsRuntimeException: enter");
        HBaseConnectionStub stub = new HBaseConnectionStub(mockConnection, maxRetries) {
            @Override
            protected synchronized Connection connect(Configuration config)
            {
                throw new RuntimeException("Connection failed");
            }
        };
        when(mockConnection.getAdmin()).thenThrow(new RuntimeException("Retryable"));

        assertThrows(RuntimeException.class, () -> stub.listNamespaceDescriptors());
        assertFalse("Connection should be unhealthy", stub.isHealthy());
        assertEquals("Retries should be maxed out", maxRetries, stub.getRetries());
        logger.info("listNamespaceDescriptors_whenReconnectThrows_marksConnectionUnhealthyAndThrowsRuntimeException: exit");
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

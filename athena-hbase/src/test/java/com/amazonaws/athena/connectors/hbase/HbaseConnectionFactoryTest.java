package com.amazonaws.athena.connectors.hbase;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HbaseConnectionFactoryTest
{
    private HbaseConnectionFactory connectionFactory;

    @Before
    public void setUp()
            throws Exception
    {
        connectionFactory = new HbaseConnectionFactory();
    }

    @Test
    public void clientCacheHitTest()
            throws IOException
    {
        Connection mockConn = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockConn.getAdmin()).thenReturn(mockAdmin);

        connectionFactory.addConnection("conStr", mockConn);
        Connection conn = connectionFactory.getOrCreateConn("conStr");

        assertEquals(mockConn, conn);
        verify(mockConn, times(1)).getAdmin();
        verify(mockAdmin, times(1)).listTableNames();
    }
}
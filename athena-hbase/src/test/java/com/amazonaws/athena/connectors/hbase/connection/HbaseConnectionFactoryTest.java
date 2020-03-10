/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase.connection;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HbaseConnectionFactoryTest
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseConnectionFactoryTest.class);

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
        logger.info("clientCacheHitTest: enter");
        HBaseConnection mockConn = mock(HBaseConnection.class);
        when(mockConn.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});
        when(mockConn.isHealthy()).thenReturn(true);

        connectionFactory.addConnection("conStr", mockConn);
        HBaseConnection conn = connectionFactory.getOrCreateConn("conStr");

        assertEquals(mockConn, conn);
        verify(mockConn, times(1)).listNamespaceDescriptors();
        verify(mockConn, times(1)).isHealthy();
        logger.info("clientCacheHitTest: exit");
    }
}

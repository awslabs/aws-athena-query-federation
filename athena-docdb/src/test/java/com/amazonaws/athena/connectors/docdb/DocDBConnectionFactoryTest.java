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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DocDBConnectionFactoryTest
{
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
}

/*-
 * #%L
 * athena-vertica
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
package com.amazonaws.athena.connectors.vertica;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class VerticaConnectionFactoryTest {
    private VerticaConnectionFactory connectionFactory;
    private int CONNECTION_TIMEOUT = 60;

    @Before
    public void setUp()
    {
        connectionFactory = new VerticaConnectionFactory();
    }

    @Test
    public void clientCacheTest() throws SQLException
    {
        Connection mockConnection =  mock(Connection.class);
        when(mockConnection.isValid(CONNECTION_TIMEOUT)).thenReturn(true);

        connectionFactory.addConnection("conStr", mockConnection);
        Connection connection = connectionFactory.getOrCreateConn("conStr");

        assertEquals(mockConnection, connection);

    }

}

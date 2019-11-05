/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.connectors.athena.jdbc.connection;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DatabaseConnectionConfigBuilderTest
{
    private static final String CONNECTION_STRING1 = "mysql://jdbc:mysql://hostname/${testSecret}";
    private static final String CONNECTION_STRING2 = "postgres://jdbc:postgresql://hostname/user=testUser&password=testPassword";

    @Test
    public void build()
    {
        DatabaseConnectionConfig expectedDatabase1 = new DatabaseConnectionConfig("testCatalog1", JdbcConnectionFactory.DatabaseEngine.MYSQL,
                "jdbc:mysql://hostname/${testSecret}", "testSecret");
        DatabaseConnectionConfig expectedDatabase2 = new DatabaseConnectionConfig("testCatalog2", JdbcConnectionFactory.DatabaseEngine.POSTGRES,
                "jdbc:postgresql://hostname/user=testUser&password=testPassword");
        DatabaseConnectionConfig defaultConnection = new DatabaseConnectionConfig("default", JdbcConnectionFactory.DatabaseEngine.POSTGRES,
                "jdbc:postgresql://hostname/user=testUser&password=testPassword");

        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder()
                .properties(ImmutableMap.of(
                        "default", CONNECTION_STRING2,
                        "testCatalog1_connection_string", CONNECTION_STRING1,
                        "testCatalog2_connection_string", CONNECTION_STRING2))
                .build();

        Assert.assertEquals(Arrays.asList(defaultConnection, expectedDatabase1, expectedDatabase2), databaseConnectionConfigs);
    }

    @Test(expected = RuntimeException.class)
    public void buildInvalidConnectionString()
    {
        new DatabaseConnectionConfigBuilder().properties(Collections.singletonMap("default", "malformedUrl")).build();
    }

    @Test(expected = RuntimeException.class)
    public void buildWithNoDefault()
    {
        new DatabaseConnectionConfigBuilder().properties(Collections.singletonMap("testDb_connection_string", CONNECTION_STRING1)).build();
    }

    @Test(expected = RuntimeException.class)
    public void buildMalformedConnectionString()
    {
        new DatabaseConnectionConfigBuilder().properties(Collections.singletonMap("testDb_connection_string", null)).build();
    }
}

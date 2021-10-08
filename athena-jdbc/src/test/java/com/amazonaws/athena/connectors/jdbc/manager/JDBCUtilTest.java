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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connectors.jdbc.postgresql.PostGreSqlMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.postgresql.PostGreSqlRecordHandler;
import com.amazonaws.athena.connectors.jdbc.mysql.MySqlMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.mysql.MySqlRecordHandler;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class JDBCUtilTest
{
    private static final int PORT = 1111;
    private static final String CONNECTION_STRING1 = "mysql://jdbc:mysql://hostname/${testSecret}";
    private static final String CONNECTION_STRING2 = "postgres://jdbc:postgresql://hostname/user=testUser&password=testPassword";


    @Test
    public void createJdbcMetadataHandlerMap()
    {
        Map<String, JdbcMetadataHandler> catalogs = JDBCUtil.createJdbcMetadataHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("testCatalog2_connection_string", CONNECTION_STRING2)
                .put("default", CONNECTION_STRING2)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build());

        Assert.assertEquals(4, catalogs.size());
        Assert.assertEquals(catalogs.get("testCatalog1").getClass(), MySqlMetadataHandler.class);
        Assert.assertEquals(catalogs.get("testCatalog2").getClass(), PostGreSqlMetadataHandler.class);
        Assert.assertEquals(catalogs.get("lambda:functionName").getClass(), PostGreSqlMetadataHandler.class);
    }

    @Test(expected = RuntimeException.class)
    public void createJdbcMetadataHandlerEmptyConnectionStrings()
    {
        JDBCUtil.createJdbcMetadataHandlerMap(Collections.emptyMap());
    }

    @Test(expected = RuntimeException.class)
    public void createJdbcMetadataHandlerNoDefault()
    {
        JDBCUtil.createJdbcMetadataHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("testCatalog2_connection_string", CONNECTION_STRING2)
                .build());
    }


    @Test
    public void createJdbcRecordHandlerMap()
    {
        Map<String, JdbcRecordHandler> catalogs = JDBCUtil.createJdbcRecordHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("testCatalog2_connection_string", CONNECTION_STRING2)
                .put("default", CONNECTION_STRING2)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build());

        Assert.assertEquals(catalogs.get("testCatalog1").getClass(), MySqlRecordHandler.class);
        Assert.assertEquals(catalogs.get("testCatalog2").getClass(), PostGreSqlRecordHandler.class);
        Assert.assertEquals(catalogs.get("lambda:functionName").getClass(), PostGreSqlRecordHandler.class);
    }

    @Test(expected = RuntimeException.class)
    public void createJdbcRecordHandlerMapEmptyConnectionStrings()
    {
        JDBCUtil.createJdbcRecordHandlerMap(Collections.emptyMap());
    }

    @Test(expected = RuntimeException.class)
    public void createJdbcRecordHandlerMapNoDefault()
    {
        JDBCUtil.createJdbcRecordHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("testCatalog2_connection_string", CONNECTION_STRING2)
                .build());
    }
}

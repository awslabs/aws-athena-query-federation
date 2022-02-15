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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

public class JDBCUtilTest
{
    private static final String CONNECTION_STRING1 = "fakedatabase://jdbc:fakedatabase://hostname/${testSecret}";
    private static final String CONNECTION_STRING2 = "notrealdb://jdbc:notrealdb://hostname/user=testUser&password=testPassword";

    class FakeDatabaseJdbcMetadataHandler extends JdbcMetadataHandler {

        public FakeDatabaseJdbcMetadataHandler(String sourceType)
        {
            super(sourceType);
        }

        @Override
        public Schema getPartitionSchema(String catalogName)
        {
            return null;
        }

        @Override
        public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
                throws Exception
        {

        }

        @Override
        public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
        {
            return null;
        }
    }

    class FakeDatabaseJdbcRecordHandler extends JdbcRecordHandler {
        protected FakeDatabaseJdbcRecordHandler(String sourceType)
        {
            super(sourceType);
        }

        @Override
        public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
                throws SQLException
        {
            return null;
        }
    }

    class FakeDatabaseMetadataHandlerFactory implements JdbcMetadataHandlerFactory {

        @Override
        public String getEngine()
        {
            return "fakedatabase";
        }

        @Override
        public JdbcMetadataHandler createJdbcMetadataHandler(DatabaseConnectionConfig config)
        {
            return new FakeDatabaseJdbcMetadataHandler("fakedatabase");
        }
    }

    class FakeDatabaseRecordHandlerFactory implements JdbcRecordHandlerFactory {

        @Override
        public String getEngine()
        {
            return "fakedatabase";
        }

        @Override
        public JdbcRecordHandler createJdbcRecordHandler(DatabaseConnectionConfig config)
        {
            return new FakeDatabaseJdbcRecordHandler("fakedatabase");
        }
    }

    @Test
    public void createJdbcMetadataHandlerMap()
    {
        Map<String, JdbcMetadataHandler> catalogs = JDBCUtil.createJdbcMetadataHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("default", CONNECTION_STRING1)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build(), new FakeDatabaseMetadataHandlerFactory());

        Assert.assertEquals(3, catalogs.size());
        Assert.assertEquals(catalogs.get("testCatalog1").getClass(), FakeDatabaseJdbcMetadataHandler.class);
        Assert.assertEquals(catalogs.get("lambda:functionName").getClass(), FakeDatabaseJdbcMetadataHandler.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createJdbcMetadataHandlerMapDifferentDatabasesThrows()
    {
        Map<String, JdbcMetadataHandler> catalogs = JDBCUtil.createJdbcMetadataHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("testCatalog2_connection_string", CONNECTION_STRING2)
                .put("default", CONNECTION_STRING2)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build(), new FakeDatabaseMetadataHandlerFactory());
    }

    @Test(expected = RuntimeException.class)
    public void createJdbcMetadataHandlerEmptyConnectionStrings()
    {
        JDBCUtil.createJdbcMetadataHandlerMap(Collections.emptyMap(), new FakeDatabaseMetadataHandlerFactory());
    }

    @Test(expected = RuntimeException.class)
    public void createJdbcMetadataHandlerNoDefault()
    {
        JDBCUtil.createJdbcMetadataHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("testCatalog2_connection_string", CONNECTION_STRING2)
                .put("default", CONNECTION_STRING2)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build(), new FakeDatabaseMetadataHandlerFactory());
    }

    @Test
    public void createJdbcRecordHandlerMap()
    {
        Map<String, JdbcRecordHandler> catalogs = JDBCUtil.createJdbcRecordHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("default", CONNECTION_STRING1)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build(), new FakeDatabaseRecordHandlerFactory());

        Assert.assertEquals(catalogs.get("testCatalog1").getClass(), FakeDatabaseJdbcRecordHandler.class);
        Assert.assertEquals(catalogs.get("lambda:functionName").getClass(), FakeDatabaseJdbcRecordHandler.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createJdbcRecordHandlerMapDifferentDatabasesThrows()
    {
        Map<String, JdbcRecordHandler> catalogs = JDBCUtil.createJdbcRecordHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("testCatalog2_connection_string", CONNECTION_STRING2)
                .put("default", CONNECTION_STRING2)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build(), new FakeDatabaseRecordHandlerFactory());
    }

    @Test(expected = RuntimeException.class)
    public void createJdbcRecordHandlerMapEmptyConnectionStrings()
    {
        JDBCUtil.createJdbcRecordHandlerMap(Collections.emptyMap(), new FakeDatabaseRecordHandlerFactory());
    }

    @Test(expected = RuntimeException.class)
    public void createJdbcRecordHandlerMapNoDefault()
    {
        JDBCUtil.createJdbcRecordHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put("testCatalog2_connection_string", CONNECTION_STRING2)
                .build(), new FakeDatabaseRecordHandlerFactory());
    }
}

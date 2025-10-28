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
    private static final String DEFAULT="default";
    private static final String FAKE_DATABASE="fakedatabase";
    private static final String TEST_SCHEMA="testSchema";
    private static final String TEST_TABLE="testTable";


    class FakeDatabaseJdbcMetadataHandler extends JdbcMetadataHandler {

        public FakeDatabaseJdbcMetadataHandler(String sourceType, java.util.Map<String, String> configOptions)
        {
            super(sourceType, configOptions);
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
        protected FakeDatabaseJdbcRecordHandler(String sourceType, java.util.Map<String, String> configOptions)
        {
            super(sourceType, configOptions);
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
            return FAKE_DATABASE;
        }

        @Override
        public JdbcMetadataHandler createJdbcMetadataHandler(DatabaseConnectionConfig config, java.util.Map<String, String> configOptions)
        {
            return new FakeDatabaseJdbcMetadataHandler(FAKE_DATABASE, configOptions);
        }
    }

    class FakeDatabaseRecordHandlerFactory implements JdbcRecordHandlerFactory {

        @Override
        public String getEngine()
        {
            return FAKE_DATABASE;
        }

        @Override
        public JdbcRecordHandler createJdbcRecordHandler(DatabaseConnectionConfig config, java.util.Map<String, String> configOptions)
        {
            return new FakeDatabaseJdbcRecordHandler(FAKE_DATABASE, configOptions);
        }
    }

    @Test
    public void createJdbcMetadataHandlerMap()
    {
        Map<String, JdbcMetadataHandler> catalogs = JDBCUtil.createJdbcMetadataHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put(DEFAULT, CONNECTION_STRING1)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build(), new FakeDatabaseMetadataHandlerFactory());

        Assert.assertEquals(3, catalogs.size());
        Assert.assertEquals(FakeDatabaseJdbcMetadataHandler.class, catalogs.get("testCatalog1").getClass());
        Assert.assertEquals(FakeDatabaseJdbcMetadataHandler.class, catalogs.get("lambda:functionName").getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createJdbcMetadataHandlerMapDifferentDatabasesThrows()
    {
        // Mux with different database types is not supported.
        JDBCUtil.createJdbcMetadataHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog_connection_string", CONNECTION_STRING1)
                .put("wrong_connection_string", CONNECTION_STRING2)
                .put(DEFAULT, CONNECTION_STRING1)
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
                .put(DEFAULT, CONNECTION_STRING2)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build(), new FakeDatabaseMetadataHandlerFactory());
    }

    @Test
    public void createJdbcRecordHandlerMap()
    {
        Map<String, JdbcRecordHandler> catalogs = JDBCUtil.createJdbcRecordHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog1_connection_string", CONNECTION_STRING1)
                .put(DEFAULT, CONNECTION_STRING1)
                .put("AWS_LAMBDA_FUNCTION_NAME", "functionName")
                .build(), new FakeDatabaseRecordHandlerFactory());

        Assert.assertEquals(FakeDatabaseJdbcRecordHandler.class, catalogs.get("testCatalog1").getClass());
        Assert.assertEquals(FakeDatabaseJdbcRecordHandler.class, catalogs.get("lambda:functionName").getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createJdbcRecordHandlerMapDifferentDatabasesThrows()
    {
        // Mux with different database types is not supported.
        JDBCUtil.createJdbcRecordHandlerMap(ImmutableMap.<String, String>builder()
                .put("testCatalog_connection_string", CONNECTION_STRING1)
                .put("wrong_connection_string", CONNECTION_STRING2)
                .put(DEFAULT, CONNECTION_STRING1)
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

    @Test
    public void testGetSingleDatabaseConfigFromEnv()
    {
        Map<String, String> configOptions = ImmutableMap.<String, String>builder()
                .put(DEFAULT, CONNECTION_STRING1)
                .build();
        
        DatabaseConnectionConfig config = JDBCUtil.getSingleDatabaseConfigFromEnv(FAKE_DATABASE, configOptions);
        
        Assert.assertNotNull(config);
        Assert.assertEquals(DEFAULT, config.getCatalog());
        Assert.assertEquals(FAKE_DATABASE, config.getEngine());
    }

    @Test(expected = RuntimeException.class)
    public void testGetSingleDatabaseConfigFromEnvThrowsWhenNoDefault()
    {
        Map<String, String> configOptions = ImmutableMap.<String, String>builder()
                .put("testCatalog_connection_string", CONNECTION_STRING1)
                .build();
        
        JDBCUtil.getSingleDatabaseConfigFromEnv(FAKE_DATABASE, configOptions);
    }

    @Test
    public void testGetTables() throws SQLException
    {
        Connection mockConnection = org.mockito.Mockito.mock(Connection.class);
        PreparedStatement mockStatement = org.mockito.Mockito.mock(PreparedStatement.class);
        java.sql.ResultSet mockResultSet = org.mockito.Mockito.mock(java.sql.ResultSet.class);
        
        org.mockito.Mockito.when(mockConnection.prepareStatement(org.mockito.Mockito.anyString())).thenReturn(mockStatement);
        org.mockito.Mockito.when(mockStatement.executeQuery()).thenReturn(mockResultSet);
        org.mockito.Mockito.when(mockResultSet.next()).thenReturn(true, false);
        org.mockito.Mockito.when(mockResultSet.getString("TABLE_NAME")).thenReturn(TEST_TABLE);
        org.mockito.Mockito.when(mockResultSet.getString("TABLE_SCHEM")).thenReturn(TEST_SCHEMA);
        
        java.util.List<TableName> tables = JDBCUtil.getTables(mockConnection, TEST_SCHEMA);
        
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(TEST_TABLE, tables.get(0).getTableName());
        Assert.assertEquals(TEST_SCHEMA, tables.get(0).getSchemaName());
    }

    @Test
    public void testGetTableMetadataWithSQLException() throws SQLException
    {
        PreparedStatement mockStatement = org.mockito.Mockito.mock(PreparedStatement.class);
        
        // Mock SQLException to test exception handling
        org.mockito.Mockito.when(mockStatement.executeQuery()).thenThrow(new SQLException("Test exception"));
        
        java.util.List<TableName> tables = JDBCUtil.getTableMetadata(mockStatement, "Tables");
        
        // Should return empty list when SQLException occurs
        Assert.assertTrue(tables.isEmpty());
    }

    @Test
    public void testGetSchemaTableName() throws SQLException
    {
        java.sql.ResultSet mockResultSet = org.mockito.Mockito.mock(java.sql.ResultSet.class);
        org.mockito.Mockito.when(mockResultSet.getString("TABLE_SCHEM")).thenReturn(TEST_SCHEMA);
        org.mockito.Mockito.when(mockResultSet.getString("TABLE_NAME")).thenReturn(TEST_TABLE);
        
        TableName tableName = JDBCUtil.getSchemaTableName(mockResultSet);
        
        Assert.assertEquals(TEST_SCHEMA, tableName.getSchemaName());
        Assert.assertEquals(TEST_TABLE, tableName.getTableName());
    }

    @Test(expected = RuntimeException.class)
    public void testCreateJdbcMetadataHandlerMapMissingLambdaFunctionName()
    {
        JDBCUtil.createJdbcMetadataHandlerMap(ImmutableMap.<String, String>builder()
                .put(DEFAULT, CONNECTION_STRING1)
                .build(), new FakeDatabaseMetadataHandlerFactory());
    }

    @Test(expected = RuntimeException.class)
    public void testCreateJdbcRecordHandlerMapMissingLambdaFunctionName()
    {
        JDBCUtil.createJdbcRecordHandlerMap(ImmutableMap.<String, String>builder()
                .put(DEFAULT, CONNECTION_STRING1)
                .build(), new FakeDatabaseRecordHandlerFactory());
    }

}

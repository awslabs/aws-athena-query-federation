/*-
 * #%L
 * athena-redshift
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
package com.amazonaws.athena.connectors.redshift;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.nullable;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedshiftMuxMetadataHandlerTest {

    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_ENGINE = "redshift";
    private static final String TEST_CONNECTION_STRING = "redshift://jdbc:redshift://hostname/${testSecret}";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String UNSUPPORTED_CATALOG = "unsupportedCatalog";
    private static final String CATALOG_NAME = "Catalog";
    private static final String ENGINE_NAME = "Engine";
    private static final String EXPECTED_ENGINE = RedshiftConstants.REDSHIFT_NAME;

    private BlockAllocator allocator;
    private RedshiftMetadataHandler redshiftMetadataHandler;
    private Map<String, JdbcMetadataHandler> metadataHandlerMap;
    private JdbcMetadataHandler jdbcMetadataHandler;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private QueryStatusChecker queryStatusChecker;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private RedshiftMetadataHandlerFactory redshiftMetadataHandlerFactory;
    private Map<String, String> configOptions;

    @Before
    public void setup() {
        this.allocator = new BlockAllocatorImpl();
        this.redshiftMetadataHandler = Mockito.mock(RedshiftMetadataHandler.class);
        this.metadataHandlerMap = Collections.singletonMap(TEST_ENGINE, this.redshiftMetadataHandler);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE,
                TEST_CONNECTION_STRING, TEST_SECRET);
        this.jdbcMetadataHandler = new RedshiftMuxMetadataHandler(this.secretsManager, this.athena, this.jdbcConnectionFactory, this.metadataHandlerMap, databaseConnectionConfig, com.google.common.collect.ImmutableMap.of());
        this.redshiftMetadataHandlerFactory = new RedshiftMetadataHandlerFactory();
        this.configOptions = new HashMap<>();
    }

    @Test
    public void testGetEngine() {
        assertEquals(EXPECTED_ENGINE, this.redshiftMetadataHandlerFactory.getEngine());
    }

    @Test
    public void testCreateJdbcMetadataHandler() {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(CATALOG_NAME, ENGINE_NAME, TEST_CONNECTION_STRING);
        JdbcMetadataHandler actualCreateJdbcMetadataHandlerResult = this.redshiftMetadataHandlerFactory.createJdbcMetadataHandler(config, this.configOptions);

        assertThat(actualCreateJdbcMetadataHandlerResult)
                .as("Handler should be an instance of RedshiftMetadataHandler")
                .isInstanceOf(RedshiftMetadataHandler.class);


        List<Field> fields = actualCreateJdbcMetadataHandlerResult.getPartitionSchema(CATALOG_NAME).getFields();
        assertEquals(2, fields.size());
        FieldType expectedFieldType = fields.get(0).getFieldType();
        assertEquals(expectedFieldType, fields.get(1).getFieldType());
    }

    @Test
    public void doListSchemaNames() throws Exception {
        ListSchemasRequest listSchemasRequest = Mockito.mock(ListSchemasRequest.class);
        Mockito.when(listSchemasRequest.getCatalogName()).thenReturn(TEST_ENGINE);
        this.jdbcMetadataHandler.doListSchemaNames(this.allocator, listSchemasRequest);
        Mockito.verify(this.redshiftMetadataHandler, Mockito.times(1)).doListSchemaNames(Mockito.eq(this.allocator), Mockito.eq(listSchemasRequest));
    }

    @Test
    public void doListTables()
            throws Exception
    {
        ListTablesRequest listTablesRequest = Mockito.mock(ListTablesRequest.class);
        Mockito.when(listTablesRequest.getCatalogName()).thenReturn(TEST_ENGINE);
        this.jdbcMetadataHandler.doListTables(this.allocator, listTablesRequest);
        Mockito.verify(this.redshiftMetadataHandler, Mockito.times(1)).doListTables(Mockito.eq(this.allocator), Mockito.eq(listTablesRequest));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        GetTableRequest getTableRequest = Mockito.mock(GetTableRequest.class);
        Mockito.when(getTableRequest.getCatalogName()).thenReturn(TEST_ENGINE);
        this.jdbcMetadataHandler.doGetTable(this.allocator, getTableRequest);
        Mockito.verify(this.redshiftMetadataHandler, Mockito.times(1)).doGetTable(Mockito.eq(this.allocator), Mockito.eq(getTableRequest));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        GetTableLayoutRequest getTableLayoutRequest = Mockito.mock(GetTableLayoutRequest.class);
        Mockito.when(getTableLayoutRequest.getTableName()).thenReturn(new TableName(TEST_SCHEMA, TEST_TABLE));
        Mockito.when(getTableLayoutRequest.getCatalogName()).thenReturn(TEST_ENGINE);
        this.jdbcMetadataHandler.doGetTableLayout(this.allocator, getTableLayoutRequest);
        Mockito.verify(this.redshiftMetadataHandler, Mockito.times(1)).doGetTableLayout(Mockito.eq(this.allocator), Mockito.eq(getTableLayoutRequest));
    }

    @Test
    public void getPartitionSchema()
    {
        this.jdbcMetadataHandler.getPartitionSchema(TEST_ENGINE);
        Mockito.verify(this.redshiftMetadataHandler, Mockito.times(1)).getPartitionSchema(Mockito.eq(TEST_ENGINE));
    }

    @Test(expected = RuntimeException.class)
    public void getPartitionSchemaForUnsupportedCatalog()
    {
        this.jdbcMetadataHandler.getPartitionSchema(UNSUPPORTED_CATALOG);
    }


    @Test
    public void getPartitions()
            throws Exception
    {
        GetTableLayoutRequest getTableLayoutRequest = Mockito.mock(GetTableLayoutRequest.class);
        Mockito.when(getTableLayoutRequest.getCatalogName()).thenReturn(TEST_ENGINE);
        this.jdbcMetadataHandler.getPartitions(Mockito.mock(BlockWriter.class), getTableLayoutRequest, queryStatusChecker);
        Mockito.verify(this.redshiftMetadataHandler, Mockito.times(1)).getPartitions(nullable(BlockWriter.class), Mockito.eq(getTableLayoutRequest), Mockito.eq(queryStatusChecker));
    }

    @Test
    public void doGetSplits()
    {
        GetSplitsRequest getSplitsRequest = Mockito.mock(GetSplitsRequest.class);
        Mockito.when(getSplitsRequest.getCatalogName()).thenReturn(TEST_ENGINE);
        this.jdbcMetadataHandler.doGetSplits(this.allocator, getSplitsRequest);
        Mockito.verify(this.redshiftMetadataHandler, Mockito.times(1)).doGetSplits(Mockito.eq(this.allocator), Mockito.eq(getSplitsRequest));
    }
}

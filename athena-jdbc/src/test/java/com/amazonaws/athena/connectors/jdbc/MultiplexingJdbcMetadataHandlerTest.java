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
package com.amazonaws.athena.connectors.jdbc;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;

public class MultiplexingJdbcMetadataHandlerTest
{
    private Map<String, JdbcMetadataHandler> metadataHandlerMap;
    private JdbcMetadataHandler fakeDatabaseHandler;
    private JdbcMetadataHandler jdbcMetadataHandler;
    private BlockAllocator allocator;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private QueryStatusChecker queryStatusChecker;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private DatabaseConnectionConfig databaseConnectionConfig;

    private static final String FAKE_DATABASE = "fakedatabase";
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SECRET = "testSecret";
    private static final String UNSUPPORTED_CATALOG = "unsupportedCatalog";
    private static final String CONNECTION_STRING = FAKE_DATABASE + "://jdbc:" + FAKE_DATABASE + "://hostname/${" + TEST_SECRET + "}";
    private static final int MAX_CATALOGS = 100;
    private static final int TOO_MANY_CATALOGS = 101;

    @Before
    public void setup()
    {
        this.allocator = new BlockAllocatorImpl();
        this.fakeDatabaseHandler = Mockito.mock(JdbcMetadataHandler.class);
        this.metadataHandlerMap = Collections.singletonMap(FAKE_DATABASE, this.fakeDatabaseHandler);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, FAKE_DATABASE, CONNECTION_STRING, TEST_SECRET);
        this.jdbcMetadataHandler = new MultiplexingJdbcMetadataHandler(this.secretsManager, this.athena, this.jdbcConnectionFactory, this.metadataHandlerMap, databaseConnectionConfig, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        ListSchemasRequest listSchemasRequest = Mockito.mock(ListSchemasRequest.class);
        Mockito.when(listSchemasRequest.getCatalogName()).thenReturn(FAKE_DATABASE);
        this.jdbcMetadataHandler.doListSchemaNames(this.allocator, listSchemasRequest);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).doListSchemaNames(Mockito.eq(this.allocator), Mockito.eq(listSchemasRequest));
    }

    @Test
    public void doListTables()
            throws Exception
    {
        ListTablesRequest listTablesRequest = Mockito.mock(ListTablesRequest.class);
        Mockito.when(listTablesRequest.getCatalogName()).thenReturn(FAKE_DATABASE);
        this.jdbcMetadataHandler.doListTables(this.allocator, listTablesRequest);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).doListTables(Mockito.eq(this.allocator), Mockito.eq(listTablesRequest));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        GetTableRequest getTableRequest = Mockito.mock(GetTableRequest.class);
        Mockito.when(getTableRequest.getCatalogName()).thenReturn(FAKE_DATABASE);
        this.jdbcMetadataHandler.doGetTable(this.allocator, getTableRequest);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).doGetTable(Mockito.eq(this.allocator), Mockito.eq(getTableRequest));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        GetTableLayoutRequest getTableLayoutRequest = Mockito.mock(GetTableLayoutRequest.class);
        Mockito.when(getTableLayoutRequest.getTableName()).thenReturn(new TableName("testSchema", "testTable"));
        Mockito.when(getTableLayoutRequest.getCatalogName()).thenReturn(FAKE_DATABASE);
        this.jdbcMetadataHandler.doGetTableLayout(this.allocator, getTableLayoutRequest);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).doGetTableLayout(Mockito.eq(this.allocator), Mockito.eq(getTableLayoutRequest));
    }

    @Test
    public void getPartitionSchema()
    {
        this.jdbcMetadataHandler.getPartitionSchema(FAKE_DATABASE);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).getPartitionSchema(Mockito.eq(FAKE_DATABASE));
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
        Mockito.when(getTableLayoutRequest.getCatalogName()).thenReturn(FAKE_DATABASE);
        this.jdbcMetadataHandler.getPartitions(Mockito.mock(BlockWriter.class), getTableLayoutRequest, queryStatusChecker);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).getPartitions(nullable(BlockWriter.class), Mockito.eq(getTableLayoutRequest), Mockito.eq(queryStatusChecker));
    }

    @Test
    public void doGetSplits()
    {
        GetSplitsRequest getSplitsRequest = Mockito.mock(GetSplitsRequest.class);
        Mockito.when(getSplitsRequest.getCatalogName()).thenReturn(FAKE_DATABASE);
        this.jdbcMetadataHandler.doGetSplits(this.allocator, getSplitsRequest);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).doGetSplits(Mockito.eq(this.allocator), Mockito.eq(getSplitsRequest));
    }

    @Test
    public void testConstructor_withTooManyHandlers_shouldThrowException() {
        metadataHandlerMap = new HashMap<>();
        for (int i = 0; i < TOO_MANY_CATALOGS; i++) {
            metadataHandlerMap.put("catalog" + i, fakeDatabaseHandler);
        }

        AthenaConnectorException exception = assertThrows(AthenaConnectorException.class, () ->
                new MultiplexingJdbcMetadataHandler(
                        secretsManager,
                        athena,
                        jdbcConnectionFactory,
                        metadataHandlerMap,
                        databaseConnectionConfig,
                        com.google.common.collect.ImmutableMap.of()
                )
        );
        assertTrue(exception.getMessage().contains("Max " + MAX_CATALOGS + " catalogs supported in multiplexer"));
    }

    @Test
    public void testDoGetQueryPassthroughSchema()
            throws Exception
    {
        GetTableRequest getTableRequest = Mockito.mock(GetTableRequest.class);
        Mockito.when(getTableRequest.getCatalogName()).thenReturn(FAKE_DATABASE);
        this.jdbcMetadataHandler.doGetQueryPassthroughSchema(this.allocator, getTableRequest);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).doGetQueryPassthroughSchema(Mockito.eq(this.allocator), Mockito.eq(getTableRequest));
    }

    @Test
    public void testDoGetDataSourceCapabilities()
    {
        GetDataSourceCapabilitiesRequest getDataSourceCapabilitiesRequest = Mockito.mock(GetDataSourceCapabilitiesRequest.class);
        Mockito.when(getDataSourceCapabilitiesRequest.getCatalogName()).thenReturn(FAKE_DATABASE);
        this.jdbcMetadataHandler.doGetDataSourceCapabilities(this.allocator, getDataSourceCapabilitiesRequest);
        Mockito.verify(this.fakeDatabaseHandler, Mockito.times(1)).doGetDataSourceCapabilities(Mockito.eq(this.allocator), Mockito.eq(getDataSourceCapabilitiesRequest));
    }
}

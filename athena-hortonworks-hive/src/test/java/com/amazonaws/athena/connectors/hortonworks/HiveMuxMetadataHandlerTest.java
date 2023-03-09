/*-
 * #%L
 * aathena-Hortonworks-hive
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

package com.amazonaws.athena.connectors.hortonworks;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.testng.Assert;

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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;

import static org.mockito.ArgumentMatchers.nullable;

public class HiveMuxMetadataHandlerTest
{
    private Map<String, JdbcMetadataHandler> metadataHandlerMap;
    private HiveMetadataHandler hiveMetadataHandler;
    private JdbcMetadataHandler jdbcMetadataHandler;
    private BlockAllocator allocator;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private QueryStatusChecker queryStatusChecker;
    private JdbcConnectionFactory jdbcConnectionFactory;
    @BeforeClass
    public static void dataSetUP() {
        System.setProperty("aws.region", "us-west-2");
    }
    @Before
    public void setup()
    {
        this.allocator = new BlockAllocatorImpl();
        this.hiveMetadataHandler = Mockito.mock(HiveMetadataHandler.class);
        this.metadataHandlerMap = Collections.singletonMap("metaHive", this.hiveMetadataHandler);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", HiveConstants.HIVE_NAME,
                "hive2://jdbc:hive2://54.89.6.2:10000/authena;AuthMech=3;${testSecret}", "testSecret");
        this.jdbcMetadataHandler = new HiveMuxMetadataHandler(this.secretsManager, this.athena, this.jdbcConnectionFactory, this.metadataHandlerMap, databaseConnectionConfig, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        ListSchemasRequest listSchemasRequest = Mockito.mock(ListSchemasRequest.class);
        Mockito.when(listSchemasRequest.getCatalogName()).thenReturn("metaHive");
        this.jdbcMetadataHandler.doListSchemaNames(this.allocator, listSchemasRequest);
        Mockito.verify(this.hiveMetadataHandler, Mockito.times(1)).doListSchemaNames(Mockito.eq(this.allocator), Mockito.eq(listSchemasRequest));
    }

    @Test
    public void doListTables()
            throws Exception
    {
        ListTablesRequest listTablesRequest = Mockito.mock(ListTablesRequest.class);
        Mockito.when(listTablesRequest.getCatalogName()).thenReturn("metaHive");
        this.jdbcMetadataHandler.doListTables(this.allocator, listTablesRequest);
        Mockito.verify(this.hiveMetadataHandler, Mockito.times(1)).doListTables(Mockito.eq(this.allocator), Mockito.eq(listTablesRequest));
    }

    @Test
    public void doGetTable() throws Exception
    {
        GetTableRequest getTableRequest = Mockito.mock(GetTableRequest.class);
        Mockito.when(getTableRequest.getCatalogName()).thenReturn("metaHive");
        this.jdbcMetadataHandler.doGetTable(this.allocator, getTableRequest);
        Mockito.verify(this.hiveMetadataHandler, Mockito.times(1)).doGetTable(Mockito.eq(this.allocator), Mockito.eq(getTableRequest));
    }
    @Test
    public void maxCatalogTest() {
        Map<String, JdbcMetadataHandler> metadataHandlersMap = new HashMap<String, JdbcMetadataHandler>();
        for (int jdbcMetadataHandlerCount = 0; jdbcMetadataHandlerCount <= 100; jdbcMetadataHandlerCount++) {
            metadataHandlersMap.put("metaHive" + jdbcMetadataHandlerCount, this.hiveMetadataHandler);
        }
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog1",
                HiveConstants.HIVE_NAME,
                "hive2://jdbc:hive2://54.89.6.2:10000/authena;AuthMech=3;${testSecret}", "testSecret");
        try {
            new HiveMuxMetadataHandler(this.secretsManager, this.athena, this.jdbcConnectionFactory,
                    metadataHandlersMap, databaseConnectionConfig, com.google.common.collect.ImmutableMap.of());
        } catch (Exception e) {
            e.getMessage();
            Assert.assertTrue(e.getMessage().contains("Max 100 catalogs supported in multiplexer."));
        }
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        GetTableLayoutRequest getTableLayoutRequest = Mockito.mock(GetTableLayoutRequest.class);
        Mockito.when(getTableLayoutRequest.getTableName()).thenReturn(new TableName("testSchema", "testTable"));
        Mockito.when(getTableLayoutRequest.getCatalogName()).thenReturn("metaHive");
        this.jdbcMetadataHandler.doGetTableLayout(this.allocator, getTableLayoutRequest);
        Mockito.verify(this.hiveMetadataHandler, Mockito.times(1)).doGetTableLayout(Mockito.eq(this.allocator), Mockito.eq(getTableLayoutRequest));
    }

    @Test
    public void getPartitionSchema()
    {
        this.jdbcMetadataHandler.getPartitionSchema("metaHive");
        Mockito.verify(this.hiveMetadataHandler, Mockito.times(1)).getPartitionSchema(Mockito.eq("metaHive"));
    }

    @Test(expected = RuntimeException.class)
    public void getPartitionSchemaForUnsupportedCatalog()
    {
        this.jdbcMetadataHandler.getPartitionSchema("unsupportedCatalog");
    }


    @Test
    public void getPartitions()
            throws Exception
    {
        GetTableLayoutRequest getTableLayoutRequest = Mockito.mock(GetTableLayoutRequest.class);
        Mockito.when(getTableLayoutRequest.getCatalogName()).thenReturn("metaHive");
        this.jdbcMetadataHandler.getPartitions(Mockito.mock(BlockWriter.class), getTableLayoutRequest, queryStatusChecker);
        Mockito.verify(this.hiveMetadataHandler, Mockito.times(1)).getPartitions(nullable(BlockWriter.class), Mockito.eq(getTableLayoutRequest), Mockito.eq(queryStatusChecker));
    }

    @Test
    public void doGetSplits()
    {
        GetSplitsRequest getSplitsRequest = Mockito.mock(GetSplitsRequest.class);
        Mockito.when(getSplitsRequest.getCatalogName()).thenReturn("metaHive");
        this.jdbcMetadataHandler.doGetSplits(this.allocator, getSplitsRequest);
        Mockito.verify(this.hiveMetadataHandler, Mockito.times(1)).doGetSplits(Mockito.eq(this.allocator), Mockito.eq(getSplitsRequest));
    }
}

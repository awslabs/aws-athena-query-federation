/*-
 * #%L
 * athena-bigquery
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

package com.amazonaws.athena.connectors.bigquery;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryMetadataHandlerTest
{
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG = "catalog";
    private static final TableName TABLE_NAME = new TableName("dataset1", "table1");

    @Mock
    BigQuery bigQuery;

    private BigQueryMetadataHandler bigQueryMetadataHandler;

    private BlockAllocator blockAllocator;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        bigQueryMetadataHandler = new BigQueryMetadataHandler(bigQuery);
        blockAllocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        blockAllocator.close();
    }

    @Test
    public void testDoListSchemaNames()
    {
        final int numDatasets = 5;
        BigQueryPage<Dataset> datasetPage =
            new BigQueryPage<>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, numDatasets));
        when(bigQuery.listDatasets(any(String.class))).thenReturn(datasetPage);

        //This will test case insenstivity
        ListSchemasRequest request = new ListSchemasRequest(BigQueryTestUtils.FEDERATED_IDENTITY,
            QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME.toLowerCase());
        ListSchemasResponse schemaNames = bigQueryMetadataHandler.doListSchemaNames(blockAllocator, request);

        assertNotNull(schemaNames);
        assertEquals("Schema count does not match!", numDatasets, schemaNames.getSchemas().size());
    }

    @Test
    public void testDoListTables()
    {
        //Build mocks for Datasets
        final int numDatasets = 5;
        BigQueryPage<Dataset> datasetPage =
            new BigQueryPage<>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, numDatasets));
        when(bigQuery.listDatasets(any(String.class))).thenReturn(datasetPage);

        //Get the first dataset name.
        String datasetName = datasetPage.iterateAll().iterator().next().getDatasetId().getDataset();

        final int numTables = 5;
        BigQueryPage<Table> tablesPage =
            new BigQueryPage<>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME,
                datasetName, numTables));

        when(bigQuery.listTables(any(DatasetId.class))).thenReturn(tablesPage);

        //This will test case insenstivity
        ListTablesRequest request = new ListTablesRequest(BigQueryTestUtils.FEDERATED_IDENTITY,
            QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME.toLowerCase(),
            datasetName);
        ListTablesResponse tableNames = bigQueryMetadataHandler.doListTables(blockAllocator, request);

        assertNotNull(tableNames);
        assertEquals("Schema count does not match!", numTables, tableNames.getTables().size());
    }

    @Test
    public void testDoGetTable()
    {
        //Build mocks for Datasets
        final int numDatasets = 5;
        BigQueryPage<Dataset> datasetPage =
            new BigQueryPage<>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, numDatasets));
        when(bigQuery.listDatasets(any(String.class))).thenReturn(datasetPage);

        //Get the first dataset name.
        String datasetName = datasetPage.iterateAll().iterator().next().getDatasetId().getDataset();

        //Build mocks for Tables
        final int numTables = 5;
        BigQueryPage<Table> tablesPage =
            new BigQueryPage<>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME,
                datasetName, numTables));

        String tableName = tablesPage.iterateAll().iterator().next().getTableId().getTable();

        when(bigQuery.listTables(any(DatasetId.class))).thenReturn(tablesPage);

        Schema tableSchema = BigQueryTestUtils.getTestSchema();
        StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(tableSchema).build();

        Table table = mock(Table.class);
        when(table.getTableId()).thenReturn(TableId.of(BigQueryTestUtils.PROJECT_1_NAME, datasetName, tableName));
        when(table.getDefinition()).thenReturn(tableDefinition);
        when(bigQuery.getTable(any(TableId.class))).thenReturn(table);

        //Make the call
        GetTableRequest getTableRequest = new GetTableRequest(BigQueryTestUtils.FEDERATED_IDENTITY,
            QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME,
            new TableName(datasetName, tableName));
        GetTableResponse response = bigQueryMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        assertNotNull(response);

        //Number of Fields
        assertEquals(tableSchema.getFields().size(), response.getSchema().getFields().size());
    }

    @Test
    public void testDoGetSplits()
    {
        GetSplitsRequest request = new GetSplitsRequest(BigQueryTestUtils.FEDERATED_IDENTITY,
            QUERY_ID, CATALOG, TABLE_NAME,
            mock(Block.class), Collections.<String>emptyList(), new Constraints(new HashMap<>()), null);
        GetSplitsResponse response = bigQueryMetadataHandler.doGetSplits(blockAllocator, request);

        assertNotNull(response);
    }
}

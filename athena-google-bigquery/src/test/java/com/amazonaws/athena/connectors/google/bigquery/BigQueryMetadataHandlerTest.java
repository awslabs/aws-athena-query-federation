/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({BigQueryUtils.class})
public class BigQueryMetadataHandlerTest
{
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG = "catalog";
    private static final TableName TABLE_NAME = new TableName("dataset1", "table1");

    @Mock
    BigQuery bigQuery;

    private BigQueryMetadataHandler bigQueryMetadataHandler;
    private int UNLIMITED_PAGE_SIZE_VALUE = 50;
    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;
    private Job job;
    private JobStatus jobStatus;

    @Before
    public void setUp() throws InterruptedException {
        System.setProperty("aws.region", "us-east-1");
        MockitoAnnotations.initMocks(this);
        bigQueryMetadataHandler = new BigQueryMetadataHandler(bigQuery);
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        job = mock(Job.class);
        jobStatus = mock(JobStatus.class);
        when(bigQuery.create(any(JobInfo.class), any())).thenReturn(job);
        when(job.waitFor(any())).thenReturn(job);
        when(job.getStatus()).thenReturn(jobStatus);
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
        when(bigQuery.listDatasets(any(String.class), any(BigQuery.DatasetListOption.class))).thenReturn(datasetPage);

        //This will test case insenstivity
        ListSchemasRequest request = new ListSchemasRequest(federatedIdentity,
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

        //This will test case insenstivity
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity,
                QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME.toLowerCase(),
                datasetName, null, UNLIMITED_PAGE_SIZE_VALUE);
        when(bigQuery.listTables(any(DatasetId.class), any(BigQuery.TableListOption.class))).thenReturn(tablesPage);
        ListTablesResponse tableNames = bigQueryMetadataHandler.doListTables(blockAllocator, listTablesRequest);
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
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity,
                QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME,
                new TableName(datasetName, tableName));
        GetTableResponse response = bigQueryMetadataHandler.doGetTable(blockAllocator, getTableRequest);

        assertNotNull(response);

        //Number of Fields
        assertEquals(tableSchema.getFields().size(), response.getSchema().getFields().size());
    }

    @Test
    public void testDoGetSplits() throws Exception
    {
        BlockAllocator blockAllocator = new BlockAllocatorImpl();
        PowerMockito.mockStatic(BigQueryUtils.class);
        when(BigQueryUtils.getBigQueryClient()).thenReturn(bigQuery);
        when(BigQueryUtils.getEnvVar("concurrencyLimit")).thenReturn("10");

        GetSplitsRequest request = new GetSplitsRequest(federatedIdentity,
                QUERY_ID, CATALOG, TABLE_NAME,
                mock(Block.class), Collections.<String>emptyList(), new Constraints(new HashMap<>()), null);
        // added schema with integer column countCol
        List<Field> testSchemaFields = Arrays.asList(Field.of("countCol", LegacySQLTypeName.INTEGER));
        com.google.cloud.bigquery.Schema tableSchema = Schema.of(testSchemaFields);

        // mocked table row count as 15
        List<FieldValue> bigQueryRowValue = Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "15"));
        FieldValueList fieldValueList = FieldValueList.of(bigQueryRowValue,
                FieldList.of(testSchemaFields));
        List<FieldValueList> tableRows = Arrays.asList(fieldValueList);

        when(job.isDone()).thenReturn(false).thenReturn(true);
        Page<FieldValueList> pageNoSchema = new BigQueryPage<>(tableRows);
        TableResult result = new TableResult(tableSchema, tableRows.size(), pageNoSchema);
        when(job.getQueryResults()).thenReturn(result);

        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);

        GetSplitsResponse response = bigQueryMetadataHandler.doGetSplits(blockAllocator, request);

        assertNotNull(response);
    }

    @Test
    public void testDoListSchemaNamesForException() {
        final int numDatasets = 5;
        BigQueryPage<Dataset> datasetPage =
                new BigQueryPage<>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, numDatasets));
        when(bigQuery.listDatasets(any(String.class))).thenReturn(datasetPage);

        ListSchemasRequest request = new ListSchemasRequest(federatedIdentity,
                QUERY_ID, BigQueryTestUtils.PROJECT_1_NAME.toLowerCase());
        when(bigQueryMetadataHandler.doListSchemaNames(blockAllocator, request)).thenThrow(new BigQueryExceptions.TooManyTablesException());
        ListSchemasResponse schemaNames = bigQueryMetadataHandler.doListSchemaNames(blockAllocator, request);
        assertEquals(null, schemaNames);
    }
}

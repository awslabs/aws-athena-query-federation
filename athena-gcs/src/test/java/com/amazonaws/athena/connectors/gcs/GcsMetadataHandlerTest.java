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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.storage.AbstractStorageDatasource;
import com.amazonaws.athena.storage.datasource.CsvDatasource;
import com.amazonaws.athena.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({GcsTestUtils.class, GcsSchemaUtils.class, StorageDatasourceFactory.class, GoogleCredentials.class, GcsSchemaUtils.class, AWSSecretsManagerClientBuilder.class, ServiceAccountCredentials.class})
public class GcsMetadataHandlerTest
{
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG = "catalog";
    private static final TableName TABLE_NAME = new TableName("dataset1", "table1");
    public static final String TEST_DATA_SET = "testDataSet";
    public static final String TEST_TOKEN = "testToken";
    String datasetName = TEST_DATA_SET;
    private GcsMetadataHandler gcsMetadataHandler;
    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;

    @Mock
    GoogleCredentials credentials;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    AmazonAthena amazonAthena;

    @Mock
    GcsSchemaUtils gcsSchemaUtils;

    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;

    @Mock
    AmazonS3 amazonS3;

    @Mock
    CsvDatasource csvDatasource;

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setTestEnvironmentVariables()
    {
        environmentVariables.set("max_partitions_size", "2");
        environmentVariables.set("gcs_credential_key", "gcs_credential_keys");
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        PowerMockito.mockStatic(ServiceAccountCredentials.class);
        PowerMockito.when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        suppress(constructor(AbstractStorageDatasource.class, StorageDatasourceConfig.class));
        PowerMockito.mockStatic(StorageDatasourceFactory.class);
        PowerMockito.when(StorageDatasourceFactory.createDatasource(anyString(), Mockito.any())).thenReturn(csvDatasource);
        when(csvDatasource.getAllDatabases()).thenReturn(GcsTestUtils.getDatasetList());
        when(csvDatasource.getAllTables(anyString(), anyString(), anyInt())).thenReturn(GcsTestUtils.getTableList());
        when(csvDatasource.getStorageTable(Mockito.any(), Mockito.any())).thenReturn(Optional.of(GcsTestUtils.getTestSchemaFields()));
        when(csvDatasource.getStorageSplits(Mockito.any(), Mockito.any(), Mockito.any(), anyString(), anyString())).thenReturn(GcsTestUtils.getSplits());
        MockitoAnnotations.initMocks(this);
        suppress(constructor(MetadataHandler.class, com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory.class, com.amazonaws.services.secretsmanager.AWSSecretsManager.class, com.amazonaws.services.athena.AmazonAthena.class, java.lang.String.class, java.lang.String.class, java.lang.String.class));
        PowerMockito.mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);

        PowerMockito.mockStatic(AWSSecretsManagerClientBuilder.class);
        PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        GetSecretValueResult getSecretValueResult = new GetSecretValueResult().withVersionStages(List.of("v1")).withSecretString("{\"gcs_credential_keys\": \"test\"}");
        Mockito.when(secretsManager.getSecretValue(Mockito.any())).thenReturn(getSecretValueResult);

        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @After
    public void tearDown()
    {
        blockAllocator.close();
    }

    @Test
    public void testDoListSchemaNames() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        final int numDatasets = 5;
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, amazonAthena, "test", "test", gcsSchemaUtils, amazonS3);
        ListSchemasRequest request = new ListSchemasRequest(federatedIdentity,
                QUERY_ID, GcsTestUtils.PROJECT_1_NAME.toLowerCase());
        ListSchemasResponse schemaNames = gcsMetadataHandler.doListSchemaNames(blockAllocator, request);
        assertNotNull(schemaNames);
        assertEquals("Schema count does not match!", numDatasets, schemaNames.getSchemas().size());
    }

    @Test
    public void testDoListTables() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        //Get the first dataset name.
        final int numTables = 5;
        int UNLIMITED_PAGE_SIZE_VALUE = 50;
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity,
                QUERY_ID, GcsTestUtils.PROJECT_1_NAME.toLowerCase(),
                datasetName, TEST_TOKEN, UNLIMITED_PAGE_SIZE_VALUE);
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, amazonAthena, "test", "test", gcsSchemaUtils, amazonS3);
        ListTablesResponse tableNames = gcsMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertNotNull(tableNames);
        assertEquals("Schema count does not match!", numTables, tableNames.getTables().size());
    }

    @Test
    public void testDoGetTable() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        when(gcsSchemaUtils.buildTableSchema(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(GcsTestUtils.getTestSchema());
        List<Field> tableSchema = GcsTestUtils.getFields();
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, amazonAthena, "test", "test", gcsSchemaUtils, amazonS3);
        //Make the call
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity,
                QUERY_ID, GcsTestUtils.PROJECT_1_NAME,
                new TableName(TEST_DATA_SET, "table1"));
        GetTableResponse response = gcsMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        assertNotNull(response);
        //Number of Fields
        assertEquals(tableSchema.size(), response.getSchema().getFields().size());
    }

    @Test
    public void testDoGetSplits() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        GetSplitsRequest request = new GetSplitsRequest(federatedIdentity,
                QUERY_ID, CATALOG, TABLE_NAME,
                mock(Block.class), Collections.emptyList(), new Constraints(new HashMap<>()), null);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, amazonAthena, "test", "test", gcsSchemaUtils, amazonS3);
        GetSplitsResponse response = gcsMetadataHandler.doGetSplits(blockAllocator, request);
        assertNotNull(response);
    }

    @Test
    @Ignore
    public void testDoGetSplitsMultiSplits() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        String yearCol = "PART_ID";
        //This is the schema that ExampleMetadataHandler has laid out for a 'Partition' so we need to populate this
        //minimal set of info here.
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField(yearCol)
                .build();
        List<String> partitionCols = new ArrayList<>();
        partitionCols.add(yearCol);
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        Block partitions = blockAllocator.createBlock(schema);

        int num_partitions = 2;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(yearCol), i, i);
        }
        partitions.setRowCount(num_partitions);
        GetSplitsRequest originalReq = new GetSplitsRequest(federatedIdentity, "queryId", "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(constraintsMap), null);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, amazonAthena, "test", "test", gcsSchemaUtils, amazonS3);
        GetSplitsResponse response = gcsMetadataHandler.doGetSplits(blockAllocator, originalReq);

        assertNotNull(response);
    }

    @Test
    @Ignore
    public void testGetPartitions() throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        when(csvDatasource.getStorageSplits(Mockito.any(), anyString())).thenReturn(GcsTestUtils.getSplits());
        BlockWriter blockWriter = mock(BlockWriter.class);
        final List<String> writtenList = new ArrayList<>();
        doAnswer(l -> writtenList.add("Test")).when(blockWriter).writeRows(any(BlockWriter.RowWriter.class));
        GetTableLayoutRequest request = new GetTableLayoutRequest(federatedIdentity,
                QUERY_ID, CATALOG, TABLE_NAME, new Constraints(new HashMap<>()), GcsTestUtils.getTestSchema(), new HashSet<>(List.of("test")));
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, amazonAthena, "test", "test", gcsSchemaUtils, amazonS3);
        gcsMetadataHandler.getPartitions(blockWriter, request, null);
        assertFalse(writtenList.isEmpty());
    }

}

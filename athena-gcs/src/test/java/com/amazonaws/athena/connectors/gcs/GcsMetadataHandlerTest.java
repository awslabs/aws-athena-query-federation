/*-
 * #%L
 * athena-gcs
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
import com.amazonaws.athena.connectors.gcs.storage.StorageMetadata;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.paginators.GetDatabasesIterable;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_KEY;
import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.allocator;
import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.createColumn;
import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.createSummaryWithLValueRangeEqual;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class GcsMetadataHandlerTest
{
    public static final String PARQUET = "parquet";
    public static final String LOCATION = "gs://mydatalake1test/birthday/";
    public static final String TABLE_1 = "testtable1";
    public static final String TABLE_2 = "testtable2";
    public static final String CATALOG_NAME = "fakedatabase";
    public static final String DATABASE_NAME = "mydatalake1";
    public static final String S3_GOOGLE_CLOUD_STORAGE_FLAG = "s3://google-cloud-storage-flag";
    public static final String DATABASE_NAME1 = "s3database";
    private static final String QUERY_ID = "queryId";
    private static final String CATALOG = "catalog";
    private static final String TEST_TOKEN = "testToken";
    private static final String SCHEMA_NAME = "default";
    private static final TableName TABLE_NAME = new TableName("default", "testtable");
    @Mock
    protected Page<Blob> tables;
    @Mock
    GoogleCredentials credentials;
    private GcsMetadataHandler gcsMetadataHandler;
    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;
    @Mock
    private GlueClient awsGlue;
    @Mock
    private SecretsManagerClient secretsManager;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;
    @Mock
    private AthenaClient athena;

    private MockedStatic<StorageOptions> mockedStorageOptions;
    private MockedStatic<ServiceAccountCredentials> mockedServiceAccountCredentials;
    private MockedStatic<GoogleCredentials> mockedServiceGoogleCredentials;
    private MockedStatic<SecretsManagerClient> mockedAWSSecretsManagerClientBuilder;

    @Before
    public void setUp() throws Exception
    {

        mockedStorageOptions = mockStatic(StorageOptions.class);
        mockedServiceAccountCredentials = mockStatic(ServiceAccountCredentials.class);
        mockedServiceGoogleCredentials = mockStatic(GoogleCredentials.class);
        mockedAWSSecretsManagerClientBuilder = mockStatic(SecretsManagerClient.class);

        Storage storage = mock(Storage.class);
        Blob blob = mock(Blob.class);
        Blob blob1 = mock(Blob.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        Mockito.when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        Mockito.when(optionBuilder.setCredentials(ArgumentMatchers.any())).thenReturn(optionBuilder);
        Mockito.when(optionBuilder.build()).thenReturn(mockedOptions);
        Mockito.when(mockedOptions.getService()).thenReturn(storage);
        Mockito.when(storage.list(anyString(), Mockito.any())).thenReturn(tables);
        Mockito.when(tables.iterateAll()).thenReturn(ImmutableList.of(blob, blob1));
        Mockito.when(blob.getName()).thenReturn("data.parquet");
        Mockito.when(blob.getSize()).thenReturn(10L);
        Mockito.when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        MockitoAnnotations.initMocks(this);
        Mockito.when(GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        Mockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);

        Mockito.when(SecretsManagerClient.create()).thenReturn(secretsManager);
        GetSecretValueResponse getSecretValueResponse = GetSecretValueResponse.builder()
                .versionStages(ImmutableList.of("v1"))
                .secretString("{\"gcs_credential_keys\": \"test\"}")
                .build();
        Mockito.when(secretsManager.getSecretValue(Mockito.isA(GetSecretValueRequest.class))).thenReturn(getSecretValueResponse);
        gcsMetadataHandler = new GcsMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", awsGlue, allocator, ImmutableMap.of());
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @After
    public void tearDown()
    {
        mockedStorageOptions.close();
        mockedServiceAccountCredentials.close();
        mockedServiceGoogleCredentials.close();
        mockedAWSSecretsManagerClientBuilder.close();
    }

    @Test
    public void testDoListSchemaNames() throws Exception
    {
        GetDatabasesResponse response = GetDatabasesResponse.builder().databaseList(
                Database.builder().name(DATABASE_NAME).locationUri(S3_GOOGLE_CLOUD_STORAGE_FLAG).build(),
                Database.builder().name(DATABASE_NAME1).locationUri(S3_GOOGLE_CLOUD_STORAGE_FLAG).build()
        ).build();
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity,
                QUERY_ID, CATALOG);
        GetDatabasesIterable mockIterable = mock(GetDatabasesIterable.class);
        when(mockIterable.stream()).thenReturn(Collections.singletonList(response).stream());
        when(awsGlue.getDatabasesPaginator(any(GetDatabasesRequest.class))).thenReturn(mockIterable);
        ListSchemasResponse schemaNamesResponse = gcsMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        List<String> expectedSchemaNames = new ArrayList<>();
        expectedSchemaNames.add(DATABASE_NAME);
        expectedSchemaNames.add(DATABASE_NAME1);
        assertEquals(expectedSchemaNames, new ArrayList<>(schemaNamesResponse.getSchemas()));
    }

    @Test(expected = RuntimeException.class)
    public void testDoListSchemaNamesThrowsException() throws Exception
    {
        ListSchemasRequest listSchemasRequest = mock(ListSchemasRequest.class);
        ListSchemasResponse listSchemasResponse = gcsMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        assertNull(listSchemasResponse);
    }

    @Test
    public void testDoListTables() throws Exception
    {
        List<Table> tableList = new ArrayList<>();
        tableList.add(Table.builder().name(TABLE_1)
                .parameters(ImmutableMap.of(CLASSIFICATION_GLUE_TABLE_PARAM, PARQUET))
                .storageDescriptor(StorageDescriptor.builder()
                        .location(LOCATION)
                        .build())
                .build());
        tableList.add(Table.builder().name(TABLE_2)
                .parameters(ImmutableMap.of())
                .storageDescriptor(StorageDescriptor.builder()
                        .location(LOCATION)
                        .parameters(ImmutableMap.of(CLASSIFICATION_GLUE_TABLE_PARAM, PARQUET))
                        .build())
                .build());
        GetTablesResponse getTablesResponse = GetTablesResponse.builder().tableList(tableList).build();
        Mockito.when(awsGlue.getTables(any(GetTablesRequest.class))).thenReturn(getTablesResponse);
        ListTablesRequest listTablesRequest = new ListTablesRequest(federatedIdentity, QUERY_ID, CATALOG, SCHEMA_NAME, TEST_TOKEN, 50);
        ListTablesResponse tableNamesResponse = gcsMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertEquals(2, tableNamesResponse.getTables().size());

        //Testing pageSize = UNLIMITED_PAGE_SIZE_VALUE and nextToken is null
        listTablesRequest = new ListTablesRequest(federatedIdentity, QUERY_ID, CATALOG, SCHEMA_NAME, null, UNLIMITED_PAGE_SIZE_VALUE);
        tableNamesResponse = gcsMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertEquals(2, tableNamesResponse.getTables().size());
        assertNull(null, tableNamesResponse.getNextToken());

        //Testing next table returned of page size 1 and nextToken 1
        getTablesResponse = GetTablesResponse.builder().tableList(tableList).nextToken("1").build();
        Mockito.when(awsGlue.getTables(any(GetTablesRequest.class))).thenReturn(getTablesResponse);
        listTablesRequest = new ListTablesRequest(federatedIdentity, QUERY_ID, CATALOG, SCHEMA_NAME, TEST_TOKEN, 1);
        tableNamesResponse = gcsMetadataHandler.doListTables(blockAllocator, listTablesRequest);
        assertEquals(2, tableNamesResponse.getTables().size());
        assertEquals("1", tableNamesResponse.getNextToken());
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        Field field = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", PARQUET);
        Schema schema = new Schema(asList(field), metadataSchema);
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity, QUERY_ID, "gcs", new TableName(SCHEMA_NAME, "testtable"), Collections.emptyMap());
        List<Column> columns = ImmutableList.of(
                createColumn("name", "String")
        );
        Table table = Table.builder()
                .name(TABLE_1)
                .databaseName(DATABASE_NAME)
                .parameters(ImmutableMap.of(CLASSIFICATION_GLUE_TABLE_PARAM, PARQUET))
                .storageDescriptor(StorageDescriptor.builder()
                        .location(LOCATION)
                        .columns(Column.builder().name("name").type("String").build())
                        .build())
                .catalogId(CATALOG)
                .partitionKeys(columns)
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse getTableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                .table(table)
                .build();
        Mockito.when(awsGlue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(getTableResponse);
        StorageMetadata storageMetadata = mock(StorageMetadata.class);
        FieldUtils.writeField(gcsMetadataHandler, "datasource", storageMetadata, true);
        Mockito.when(storageMetadata.buildTableSchema(any(), any())).thenReturn(schema);
        GetTableResponse res = gcsMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        Field expectedField = res.getSchema().findField("name");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(expectedField.getType()));
    }

    @Test
    public void testGetPartitions() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().addField("id", new ArrowType.Int(64, false))
                .addField("year", new ArrowType.Utf8())
                .addField("month", new ArrowType.Utf8())
                .addField("day", new ArrowType.Utf8()).build();
        List<Column> columns = ImmutableList.of(
                createColumn("year", "varchar"),
                createColumn("month", "varchar"),
                createColumn("day", "varchar")
        );
        Table table = Table.builder()
                .name(TABLE_1)
                .databaseName(DATABASE_NAME)
                .parameters(ImmutableMap.of(CLASSIFICATION_GLUE_TABLE_PARAM, PARQUET,
                        PARTITION_PATTERN_KEY, "year=${year}/birth_month${month}/${day}")
                )
                .storageDescriptor(StorageDescriptor.builder()
                        .location(LOCATION)
                        .columns(Column.builder().build())
                        .build())
                .catalogId(CATALOG)
                .partitionKeys(columns)
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse getTableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                .table(table)
                .build();
        Mockito.when(awsGlue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(getTableResponse);
        GetTableLayoutRequest getTableLayoutRequest = Mockito.mock(GetTableLayoutRequest.class);
        Mockito.when(getTableLayoutRequest.getTableName()).thenReturn(new TableName(DATABASE_NAME, TABLE_1));
        Mockito.when(getTableLayoutRequest.getSchema()).thenReturn(schema);
        Constraints constraints = new Constraints(createSummaryWithLValueRangeEqual("year", new ArrowType.Utf8(), 2000) ,
                Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap());
        Mockito.when(getTableLayoutRequest.getConstraints()).thenReturn(constraints);
        BlockWriter blockWriter = Mockito.mock(BlockWriter.class);
        gcsMetadataHandler.getPartitions(blockWriter, getTableLayoutRequest, null);
        verify(blockWriter, times(1)).writeRows(any());
    }

    @Test
    public void testDoGetSplits() throws Exception
    {
        Block partitions = BlockUtils.newBlock(blockAllocator, "year", Types.MinorType.VARCHAR.getType(), 2000, 2001);
        GetSplitsRequest request = new GetSplitsRequest(federatedIdentity,
                QUERY_ID, CATALOG, TABLE_NAME,
                partitions, ImmutableList.of("year"), new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()), null);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        StorageDescriptor storageDescriptor = StorageDescriptor.builder().location(LOCATION).build();
        Table table = Table.builder()
                .storageDescriptor(storageDescriptor)
                .parameters(ImmutableMap.of(PARTITION_PATTERN_KEY, "year=${year}/", CLASSIFICATION_GLUE_TABLE_PARAM, PARQUET))
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse getTableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                .table(table)
                .build();
        when(awsGlue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(getTableResponse);

        GetSplitsResponse response = gcsMetadataHandler.doGetSplits(blockAllocator, request);
        assertEquals(2, response.getSplits().size());
        assertEquals(ImmutableList.of("2000", "2001"), response.getSplits().stream().map(split -> split.getProperties().get("year")).sorted().collect(Collectors.toList()));
    }

    @Test
    public void testDoGetSplitsProperty() throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("yearCol")
                .addStringField("monthCol")
                .build();
        BlockAllocatorImpl allocator = new BlockAllocatorImpl();
        Block partitions = allocator.createBlock(schema);

        int num_partitions = 4;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector("yearCol"), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector("monthCol"), i, (i % 12) + 1);
        }
        partitions.setRowCount(num_partitions);
        GetSplitsRequest request = new GetSplitsRequest(federatedIdentity,
                QUERY_ID, CATALOG, TABLE_NAME,
                partitions, ImmutableList.of("yearCol", "monthCol"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()), null);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        StorageDescriptor storageDescriptor = StorageDescriptor.builder()
                .location(LOCATION)
                .build();
        Table table = Table.builder()
                .storageDescriptor(storageDescriptor)
                .parameters(ImmutableMap.of(PARTITION_PATTERN_KEY, "year=${yearCol}/month${monthCol}/", CLASSIFICATION_GLUE_TABLE_PARAM, PARQUET))
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse getTableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                .table(table)
                .build();
        when(awsGlue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(getTableResponse);
        GetSplitsResponse response = gcsMetadataHandler.doGetSplits(blockAllocator, request);
        assertEquals(4, response.getSplits().size());
        assertEquals(ImmutableList.of("2016", "2017", "2018", "2019"), response.getSplits().stream().map(split -> split.getProperties().get("yearCol")).sorted().collect(Collectors.toList()));
        assertEquals(ImmutableList.of("1", "2", "3", "4"), response.getSplits().stream().map(split -> split.getProperties().get("monthCol")).sorted().collect(Collectors.toList()));
    }

    @Test(expected = RuntimeException.class)
    public void testDoGetSplitsException() throws Exception
    {
        Block partitions = BlockUtils.newBlock(blockAllocator, "gcs_file_format", Types.MinorType.VARCHAR.getType(), 2000, 2001);
        GetSplitsRequest request = new GetSplitsRequest(federatedIdentity,
                QUERY_ID, CATALOG, TABLE_NAME,
                partitions, ImmutableList.of("gcs_file_format"), new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()), null);
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        StorageDescriptor storageDescriptor = StorageDescriptor.builder()
                .location(LOCATION)
                .build();
        Table table = Table.builder()
                .storageDescriptor(storageDescriptor)
                .parameters(ImmutableMap.of(PARTITION_PATTERN_KEY, "year=${gcs_file_format}/", CLASSIFICATION_GLUE_TABLE_PARAM, PARQUET))
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse getTableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                .table(table)
                .build();
        when(awsGlue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(getTableResponse);
        gcsMetadataHandler.doGetSplits(blockAllocator, request);
    }
}

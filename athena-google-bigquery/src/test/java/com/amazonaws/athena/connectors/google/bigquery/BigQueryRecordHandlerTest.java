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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryTestUtils.getBlockTestSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryRecordHandlerTest.class);
    RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    private BlockAllocator allocator;
    @Mock
    BigQuery bigQuery;

    @Mock
    SecretsManagerClient awsSecretsManager;
    private String bucket = "bucket";

    private String prefix = "prefix";
    @Mock
    private AthenaClient athena;
    @Mock
    private BigQueryReadClient bigQueryReadClient;
    @Mock
    private ServerStream<ReadRowsResponse> serverStream;
    @Mock
    private ArrowSchema arrowSchema;
    private BigQueryRecordHandler bigQueryRecordHandler;
    private S3Client amazonS3;
    private S3BlockSpiller spillWriter;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private EncryptionKey encryptionKey = keyFactory.create();
    private SpillConfig spillConfig;
    private String queryId = UUID.randomUUID().toString();
    private S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(queryId)
            .withIsDirectory(true)
            .build();
    private FederatedIdentity federatedIdentity;
    private MockedStatic<BigQueryUtils> mockedStatic;
    @Mock
    private Job queryJob;

    @Before
    public void init()
    {
        System.setProperty("aws.region", "us-east-1");
        logger.info("Starting init.");
        mockedStatic = Mockito.mockStatic(BigQueryUtils.class);
        mockedStatic.when(() -> BigQueryUtils.getBigQueryClient(any(Map.class), any(String.class))).thenReturn(bigQuery);
        mockedStatic.when(() -> BigQueryUtils.getBigQueryClient(any(Map.class))).thenReturn(bigQuery);
        mockedStatic.when(() -> BigQueryUtils.getEnvBigQueryCredsSmId(any(Map.class))).thenReturn("dummySecret");
        
        // Mock the SecretsManager response
        GetSecretValueResponse secretResponse = GetSecretValueResponse.builder()
                .secretString("dummy-secret-value")
                .build();
        when(awsSecretsManager.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(secretResponse);
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        allocator = new BlockAllocatorImpl();
        amazonS3 = mock(S3Client.class);

        //Create Spill config
        spillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(encryptionKey)
                //This will force the writer to spill.
                .withMaxBlockBytes(20)
                .withMaxInlineBlockBytes(1)
                //Async Writing.
                .withNumSpillThreads(0)
                .withRequestId(UUID.randomUUID().toString())
                .withSpillLocation(s3SpillLocation)
                .build();

        schemaForRead = new Schema(BigQueryTestUtils.getTestSchemaFieldsArrow());
        spillWriter = new S3BlockSpiller(amazonS3, spillConfig, allocator, schemaForRead, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        //Mock the BigQuery Client to return Datasets, and Table Schema information.
        Table table = mock(Table.class);
        TableDefinition def = mock(TableDefinition.class);
        when(bigQuery.getTable(any())).thenReturn(table);
        when(table.getDefinition()).thenReturn(def);
        when(def.getType()).thenReturn(TableDefinition.Type.TABLE);
        
        // Mock the fixCaseForDatasetName and fixCaseForTableName methods
        mockedStatic.when(() -> BigQueryUtils.fixCaseForDatasetName(any(String.class), any(String.class), any(BigQuery.class))).thenReturn("dataset1");
        mockedStatic.when(() -> BigQueryUtils.fixCaseForTableName(any(String.class), any(String.class), any(String.class), any(BigQuery.class))).thenReturn("table1");

        //The class we want to test.
        Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
                BigQueryConstants.GCP_PROJECT_ID, "test",
                BigQueryConstants.ENV_BIG_QUERY_CREDS_SM_ID, "dummySecret"
        );
        bigQueryRecordHandler = new BigQueryRecordHandler(amazonS3, awsSecretsManager, athena, configOptions, rootAllocator);

        logger.info("Completed init.");
    }

    @After
    public void close()
    {
        mockedStatic.close();
        allocator.close();
    }

    @Test
    public void testReadWithConstraint()
            throws Exception
    {
        try (ReadRecordsRequest request = getReadRecordsRequest(Collections.emptyMap())) {
            // Mocking necessary dependencies
            ReadSession readSession = mock(ReadSession.class);
            ServerStreamingCallable ssCallable = mock(ServerStreamingCallable.class);

            // Mocking method calls
            try (MockedStatic<BigQueryReadClient> mockedReadClient = mockStatic(BigQueryReadClient.class)) {
                mockedReadClient.when(() -> BigQueryReadClient.create(any(BigQueryReadSettings.class))).thenReturn(bigQueryReadClient);
                when(bigQueryReadClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(readSession);
                when(readSession.getArrowSchema()).thenReturn(arrowSchema);

            when(readSession.getStreamsCount()).thenReturn(1);
            ReadStream readStream = mock(ReadStream.class);
            when(readSession.getStreams(anyInt())).thenReturn(readStream);
            when(readStream.getName()).thenReturn("testStream");

            // Create proper schema serialization
            Schema schema = new Schema(Arrays.asList(
                    new Field("int1", FieldType.nullable(new ArrowType.Int(32, true)), null),
                    new Field("string1", FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("bool1", FieldType.nullable(new ArrowType.Bool()), null),
                    new Field("float1", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
            ));
            
            ByteArrayOutputStream schemaOut = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(java.nio.channels.Channels.newChannel(schemaOut)), schema);

            ByteString bs = mock(ByteString.class);
            when(arrowSchema.getSerializedSchema()).thenReturn(bs);
            when(bs.toByteArray()).thenReturn(schemaOut.toByteArray());
            when(bigQueryReadClient.readRowsCallable()).thenReturn(ssCallable);

            when(ssCallable.call(any(ReadRowsRequest.class))).thenReturn(serverStream);
            
            // Create real ReadRowsResponse instead of mocking
            ReadRowsResponse realReadRowsResponse = createReadRowsResponseExample();

            when(serverStream.iterator()).thenReturn(ImmutableList.of(realReadRowsResponse).iterator());

            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

            //Execute the test
            bigQueryRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);

            //Ensure that there was a spill so that we can read the spilled block.
            assertTrue(spillWriter.spilled());
            }
        }
    }

    @Test
    public void testReadWithConstraint_QueryPassThrough_Success() throws Exception
    {
        Map<String, String> passthroughArgs = getPassthroughArgs();
        try (ReadRecordsRequest request = getReadRecordsRequest(passthroughArgs)) {
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);
            when(bigQuery.create(any(JobInfo.class))).thenReturn(queryJob);
            when(queryJob.isDone()).thenReturn(false).thenReturn(true);
            TableResult result = setupMockTableResult();

            when(queryJob.getQueryResults()).thenReturn(result);

            //Execute the test
            bigQueryRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);

            // Verify
            verify(bigQuery).create(any(JobInfo.class));
            verify(queryJob).getQueryResults();
            verify(queryStatusChecker).isQueryRunning();
        }
    }

    @Test
    public void testReadWithConstraint_QueryPassThrough_JobExists() throws Exception
    {
        Map<String, String> passthroughArgs = getPassthroughArgs();
        try (ReadRecordsRequest request = getReadRecordsRequest(passthroughArgs);
             QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class)) {

            //Job Already Exists Scenario
            when(bigQuery.create(any(JobInfo.class))).thenThrow(new BigQueryException(409, "Job Already Exists"));

            //Execute the test
            BigQueryException exception = assertThrows(BigQueryException.class, () ->
                    bigQueryRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker)
            );

            assertTrue(exception.getMessage().contains("Job Already Exists"));
            assertEquals(409, exception.getCode());
        }
    }

    /**
     * Test that queries with LIMIT and/or ORDER BY use SQL API instead of Storage API
     */
    @Test
    public void testQueryPlanWithLimitAndSort_UsesSqlApi() throws Exception
    {
        String substraitPlan = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIQGg4IARABGghhbmQ6Ym9vbBIPGg0IARACGgdvcjpib29sEhUaEwgCEAMaDWVxdWFsOmFueV9hbnkSGRoXCAIQBBoRbm90X2VxdWFsOmFueV9hbnkaiAYShQYK/gUa+wUKAgoAEvAFOu0FCgUSAwoBFhLXBRLUBQoCCgASlwQKlAQKAgoAEuQDCglpc19hY3RpdmUKC3RpbnlpbnRfY29sCgxzbWFsbGludF9jb2wKCHByaW9yaXR5CgpiaWdpbnRfY29sCglmbG9hdF9jb2wKCmRvdWJsZV9jb2wKCHJlYWxfY29sCgt2YXJjaGFyX2NvbAoIY2hhcl9jb2wKDXZhcmJpbmFyeV9jb2wKCGRhdGVfY29sCgh0aW1lX2NvbAoNdGltZXN0YW1wX2NvbAoCaWQKDGRlY2ltYWxfY29sMgoMZGVjaW1hbF9jb2wzCgtzdWJjYXRlZ29yeQoNaW50X2FycmF5X2NvbAoHbWFwX2NvbAoQbWFwX3dpdGhfZGVjaW1hbAoMbmVzdGVkX2FycmF5EtYBCgQKAhACCgQSAhACCgQaAhACCgQqAhACCgQ6AhACCgRaAhACCgRaAhACCgRSAhACCgRiAhACCgeqAQQIARgCCgRqAhACCgWCAQIQAgoFigECEAIKBYoCAhgCCgnCAQYIBBATIAIKCcIBBggCEAogAgoJwgEGCAoQEyACCgvaAQgKBGICEAIYAgoL2gEICgQqAhACGAIKEeIBDgoEYgIQAhIEKgIQAiACChbiARMKBGICEAISCcIBBggCEAogAiACChLaAQ8KC9oBCAoEKgIQAhgCGAIYAjonCgpteV9kYXRhc2V0ChlzZXJ2aWNlX3JlcXVlc3RzX25vX25vaXNlGrMBGrABCAEaBAoCEAIigwEagAEafggCGgQKAhACIjkaNxo1CAMaBAoCEAIiDBoKEggKBBICCA4iACIdGhsKGcIBFgoQQEIPAAAAAAAAAAAAAAAAABATGAQiORo3GjUIAxoECgIQAiIMGgoSCAoEEgIIDiIAIh0aGwoZwgEWChCAhB4AAAAAAAAAAAAAAAAAEBMYBCIgGh4aHAgEGgQKAhACIgoaCBIGCgISACIAIgYaBAoCCAEaChIICgQSAggOIgAYACAKEgJJRDILEEoqB2lzdGhtdXM=";

        QueryPlan queryPlan = new QueryPlan("", substraitPlan);
        Constraints constraints = new Constraints(
            Collections.emptyMap(),
            Collections.emptyList(),
            Collections.emptyList(),
            100L,
            Collections.emptyMap(),
            queryPlan
        );

        try (ReadRecordsRequest request = createReadRecordsRequestWithConstraints(constraints)) {
            // Mock SQL API response
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);
            when(bigQuery.create(any(JobInfo.class))).thenReturn(queryJob);
            when(queryJob.isDone()).thenReturn(false).thenReturn(true);
            TableResult result = setupMockTableResult();
            when(queryJob.getQueryResults()).thenReturn(result);

            // Execute test
            bigQueryRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);

            // Verify SQL API was used (not Storage API)
            verify(bigQuery).create(any(JobInfo.class));
        }
    }



    private Map<String, String> getPassthroughArgs() {
        return Map.of(
                "schemaFunctionName", "SYSTEM.QUERY",
                "QUERY", "SELECT * FROM test_table");
    }

    private ReadRecordsRequest getReadRecordsRequest(Map<String, String> passthroughArgs) {
        return new ReadRecordsRequest(
                federatedIdentity,
                BigQueryTestUtils.PROJECT_1_NAME,
                "queryId",
                new TableName("dataset1", "table1"),
                getBlockTestSchema(),
                Split.newBuilder(S3SpillLocation.newBuilder()
                                .withBucket(bucket)
                                .withPrefix(prefix)
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, passthroughArgs, null),
                0,
                0);
    }

    private ReadRecordsRequest createReadRecordsRequestWithConstraints(Constraints constraints)
    {
        return new ReadRecordsRequest(
            federatedIdentity,
            BigQueryTestUtils.PROJECT_1_NAME,
            "queryId",
            new TableName("dataset1", "table1"),
            getBlockTestSchema(),
            Split.newBuilder(S3SpillLocation.newBuilder()
                    .withBucket(bucket)
                    .withPrefix(prefix)
                    .withSplitId(UUID.randomUUID().toString())
                    .withQueryId(UUID.randomUUID().toString())
                    .withIsDirectory(true)
                    .build(),
                keyFactory.create()).build(),
            constraints,
            0,
            0);
    }

    private TableResult setupMockTableResult() {
        TableResult result = mock(TableResult.class, Mockito.RETURNS_DEEP_STUBS);
        // added schema with bool,int,string,float columns
        List<com.google.cloud.bigquery.Field> testSchemaFields = Arrays.asList(com.google.cloud.bigquery.Field.of(
                        "bool1", LegacySQLTypeName.BOOLEAN),
                com.google.cloud.bigquery.Field.of(
                        "int1", LegacySQLTypeName.INTEGER),
                com.google.cloud.bigquery.Field.of(
                        "string1", LegacySQLTypeName.STRING),
                com.google.cloud.bigquery.Field.of(
                        "float1", LegacySQLTypeName.FLOAT));
        com.google.cloud.bigquery.Schema tableSchema = com.google.cloud.bigquery.Schema.of(testSchemaFields);

        List<FieldValue> bigQueryRowValue = Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10.0"));
        FieldValueList fieldValueList = FieldValueList.of(bigQueryRowValue,
                FieldList.of(testSchemaFields));
        List<FieldValueList> tableRows = List.of(fieldValueList);

        when(result.getSchema()).thenReturn(tableSchema);
        when(result.iterateAll()).thenReturn(tableRows);
        when(result.getPageNoSchema()).thenReturn(new BigQueryPage<>(tableRows));

        return result;
    }

    public static com.google.cloud.bigquery.storage.v1.ReadRowsResponse createReadRowsResponseExample() throws Exception {
        com.google.cloud.bigquery.storage.v1.ArrowRecordBatch arrowRecordBatch = createExample();

        ReadRowsResponse build = ReadRowsResponse.newBuilder()
                .setArrowRecordBatch(arrowRecordBatch)
                .setRowCount(2)
                .build();
        return build;
    }

    public static com.google.cloud.bigquery.storage.v1.ArrowRecordBatch createExample() throws Exception {
        try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // Create schema
            Schema schema = new Schema(Arrays.asList(
                    new Field("int1", FieldType.nullable(new ArrowType.Int(32, true)), null),
                    new Field("string1", FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("bool1", FieldType.nullable(new ArrowType.Bool()), null),
                    new Field("float1", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
            ));

            // Create vectors with data
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

            IntVector intVector = (IntVector) root.getVector("int1");
            intVector.allocateNew(2);
            intVector.set(0, 42);
            intVector.set(1, 3);
            intVector.setValueCount(2);

            VarCharVector stringVector = (VarCharVector) root.getVector("string1");
            stringVector.allocateNew(2);
            stringVector.set(0, "test".getBytes(StandardCharsets.UTF_8));
            stringVector.set(1, "test1".getBytes(StandardCharsets.UTF_8));
            stringVector.setValueCount(2);

            BitVector boolVector = (BitVector) root.getVector("bool1");
            boolVector.allocateNew(2);
            boolVector.set(0, 1); // true
            boolVector.set(1, 0); // false
            boolVector.setValueCount(2);

            Float8Vector floatVector = (Float8Vector) root.getVector("float1");
            floatVector.allocateNew(2);
            floatVector.set(0, 1.0);
            floatVector.set(1, 0.0);
            floatVector.setValueCount(2);

            root.setRowCount(2);

            // Use VectorUnloader to create proper ArrowRecordBatch
            org.apache.arrow.vector.VectorUnloader unloader = new org.apache.arrow.vector.VectorUnloader(root);
            org.apache.arrow.vector.ipc.message.ArrowRecordBatch batch = unloader.getRecordBatch();

            // Serialize using MessageSerializer
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(java.nio.channels.Channels.newChannel(out)), batch);

            // Create BigQuery ArrowRecordBatch
            com.google.cloud.bigquery.storage.v1.ArrowRecordBatch recordBatch =
                    com.google.cloud.bigquery.storage.v1.ArrowRecordBatch.newBuilder()
                            .setSerializedRecordBatch(ByteString.copyFrom(out.toByteArray()))
                            .setRowCount(2)
                            .build();

            batch.close();
            root.close();
            allocator.close();

            return recordBatch;
        }
    }
}

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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryTestUtils.getBlockTestSchema;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
    AWSSecretsManager awsSecretsManager;
    private String bucket = "bucket";

    private String prefix = "prefix";
    @Mock
    private AmazonAthena athena;
    @Mock
    private BigQueryReadClient bigQueryReadClient;
    @Mock
    private ServerStream<ReadRowsResponse> serverStream;
    @Mock
    private ArrowSchema arrowSchema;
    private BigQueryRecordHandler bigQueryRecordHandler;
    private AmazonS3 amazonS3;
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
    private MockedStatic<MessageSerializer> messageSer;
    MockedConstruction<VectorSchemaRoot> mockedDefaultVectorSchemaRoot;
    MockedConstruction<VectorLoader> mockedDefaultVectorLoader;

    public List<FieldVector> getFieldVectors()
    {
        List<FieldVector> fieldVectors = new ArrayList<>();
        IntVector intVector = new IntVector("int1", rootAllocator);
        intVector.allocateNew(1024);
        intVector.setSafe(0, 42);  // Example: Set the value at index 0 to 42
        intVector.setSafe(1, 3);
        intVector.setValueCount(2);
        fieldVectors.add(intVector);
        VarCharVector varcharVector = new VarCharVector("string1", rootAllocator);
        varcharVector.allocateNew(1024);
        varcharVector.setSafe(0, "test".getBytes(StandardCharsets.UTF_8));  // Example: Set the value at index 0 to 42
        varcharVector.setSafe(1, "test1".getBytes(StandardCharsets.UTF_8));
        varcharVector.setValueCount(2);
        fieldVectors.add(varcharVector);
        BitVector bitVector = new BitVector("bool1", rootAllocator);
        bitVector.allocateNew(1024);
        bitVector.setSafe(0, 1);  // Example: Set the value at index 0 to 42
        bitVector.setSafe(1, 0);
        bitVector.setValueCount(2);
        fieldVectors.add(bitVector);
        Float8Vector float8Vector = new Float8Vector("float1", rootAllocator);
        float8Vector.allocateNew(1024);
        float8Vector.setSafe(0, 1.00f);  // Example: Set the value at index 0 to 42
        float8Vector.setSafe(1, 0.0f);
        float8Vector.setValueCount(2);
        fieldVectors.add(float8Vector);
        IntVector innerVector = new IntVector("innerVector", rootAllocator);
        innerVector.allocateNew(1024);
        innerVector.setSafe(0, 10);
        innerVector.setSafe(1, 20);
        innerVector.setSafe(2, 30);
        innerVector.setValueCount(3);

        // Create a ListVector and add the inner vector to it
        ListVector listVector = ListVector.empty("listVector", rootAllocator);
        UnionListWriter writer = listVector.getWriter();
        for (int i = 0; i < 2; i++) {
            writer.startList();
            writer.setPosition(i);
            for (int j = 0; j < 5; j++) {
                writer.writeInt(j * i);
            }
            writer.setValueCount(5);
            writer.endList();
        }
        listVector.setValueCount(2);
        fieldVectors.add(listVector);
        return fieldVectors;
    }

    @Before
    public void init()
    {
        System.setProperty("aws.region", "us-east-1");
        logger.info("Starting init.");
        mockedStatic = Mockito.mockStatic(BigQueryUtils.class, Mockito.CALLS_REAL_METHODS);
        mockedStatic.when(() -> BigQueryUtils.getBigQueryClient(any(Map.class))).thenReturn(bigQuery);
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        allocator = new BlockAllocatorImpl();
        amazonS3 = mock(AmazonS3.class);

        //Create Spill config
        spillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(encryptionKey)
                //This will be enough for a single block
                .withMaxBlockBytes(100000)
                //This will force the writer to spill.
                .withMaxInlineBlockBytes(100)
                //Async Writing.
                .withNumSpillThreads(0)
                .withRequestId(UUID.randomUUID().toString())
                .withSpillLocation(s3SpillLocation)
                .build();

        schemaForRead = new Schema(BigQueryTestUtils.getTestSchemaFieldsArrow());
        spillWriter = new S3BlockSpiller(amazonS3, spillConfig, allocator, schemaForRead, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        //Mock the BigQuery Client to return Datasets, and Table Schema information.
        BigQueryPage<Dataset> datasets = new BigQueryPage<Dataset>(BigQueryTestUtils.getDatasetList(BigQueryTestUtils.PROJECT_1_NAME, 2));
        when(bigQuery.listDatasets(nullable(String.class))).thenReturn(datasets);
        BigQueryPage<Table> tables = new BigQueryPage<Table>(BigQueryTestUtils.getTableList(BigQueryTestUtils.PROJECT_1_NAME, "dataset1", 2));
        when(bigQuery.listTables(nullable(DatasetId.class))).thenReturn(tables);
        Table table = mock(Table.class);
        TableDefinition def = mock(TableDefinition.class);
        when(bigQuery.getTable(any())).thenReturn(table);
        when(table.getDefinition()).thenReturn(def);
        when(def.getType()).thenReturn(TableDefinition.Type.TABLE);

        //The class we want to test.
        bigQueryRecordHandler = new BigQueryRecordHandler(amazonS3, awsSecretsManager, athena, com.google.common.collect.ImmutableMap.of(BigQueryConstants.GCP_PROJECT_ID, "test"), rootAllocator);

        logger.info("Completed init.");
    }

    @After
    public void close()
    {
        mockedDefaultVectorLoader.close();
        mockedDefaultVectorSchemaRoot.close();
        mockedStatic.close();
        messageSer.close();
        allocator.close();
    }

    @Test
    public void testReadWithConstraint()
            throws Exception
    {
        try (ReadRecordsRequest request = new ReadRecordsRequest(
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
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                0,          //This is ignored when directly calling readWithConstraints.
                0)) {

            // Mocking necessary dependencies
            ReadSession readSession = mock(ReadSession.class);
            ReadRowsResponse readRowsResponse = mock(ReadRowsResponse.class);
            ServerStreamingCallable ssCallable = mock(ServerStreamingCallable.class);

            // Mocking method calls
            mockStatic(BigQueryReadClient.class);
            when(BigQueryReadClient.create()).thenReturn(bigQueryReadClient);
            messageSer = mockStatic(MessageSerializer.class);
            when(MessageSerializer.deserializeSchema((ReadChannel) any())).thenReturn(BigQueryTestUtils.getBlockTestSchema());
            mockedDefaultVectorLoader = Mockito.mockConstruction(VectorLoader.class,
                    (mock, context) -> {
                        Mockito.doNothing().when(mock).load(any());
                    });
            mockedDefaultVectorSchemaRoot = Mockito.mockConstruction(VectorSchemaRoot.class,
                    (mock, context) -> {
                        when(mock.getRowCount()).thenReturn(2);
                        when(mock.getFieldVectors()).thenReturn(getFieldVectors());
                    });
            when(bigQueryReadClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(readSession);
            when(readSession.getArrowSchema()).thenReturn(arrowSchema);
            when(readSession.getStreamsCount()).thenReturn(1);
            ReadStream readStream = mock(ReadStream.class);
            when(readSession.getStreams(anyInt())).thenReturn(readStream);
            when(readStream.getName()).thenReturn("testStream");
            byte[] byteArray1 = {(byte) 0xFF};
            ByteString byteString1 = ByteString.copyFrom(byteArray1);

            ByteString bs = mock(ByteString.class);
            when(arrowSchema.getSerializedSchema()).thenReturn(bs);
            when(bs.toByteArray()).thenReturn(byteArray1);
            when(bigQueryReadClient.readRowsCallable()).thenReturn(ssCallable);
            when(ssCallable.call(any(ReadRowsRequest.class))).thenReturn(serverStream);
            when(serverStream.iterator()).thenReturn(ImmutableList.of(readRowsResponse).iterator());
            when(readRowsResponse.hasArrowRecordBatch()).thenReturn(true);
            com.google.cloud.bigquery.storage.v1.ArrowRecordBatch arrowRecordBatch = mock(com.google.cloud.bigquery.storage.v1.ArrowRecordBatch.class);
            when(readRowsResponse.getArrowRecordBatch()).thenReturn(arrowRecordBatch);
            byte[] byteArray = {(byte) 0xFF};
            ByteString byteString = ByteString.copyFrom(byteArray);
            when(arrowRecordBatch.getSerializedRecordBatch()).thenReturn(byteString);
            ArrowRecordBatch apacheArrowRecordBatch = mock(ArrowRecordBatch.class);
            when(MessageSerializer.deserializeRecordBatch(any(ReadChannel.class), any())).thenReturn(apacheArrowRecordBatch);
            Mockito.doNothing().when(apacheArrowRecordBatch).close();

            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);

            //Execute the test
            bigQueryRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);

            //Ensure that there was a spill so that we can read the spilled block.
            assertTrue(spillWriter.spilled());
        }
    }
}

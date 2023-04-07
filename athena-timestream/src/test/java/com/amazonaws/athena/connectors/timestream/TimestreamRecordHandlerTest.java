/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.proto.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.proto.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.google.common.io.ByteStreams;
import com.google.protobuf.Message;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.VIEW_METADATA_FIELD;
import static com.amazonaws.athena.connectors.timestream.TestUtils.makeMockQueryResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimestreamRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamRecordHandlerTest.class);

    private static final FederatedIdentity IDENTITY = FederatedIdentity.newBuilder().setArn("arn").setAccount("account").build();

    private TimestreamRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private AmazonS3 amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private static final String DEFAULT_CATALOG = "my_catalog";
    private static final String DEFAULT_SCHEMA = "my_schema";
    private static final String TEST_TABLE = "my_table";
    private static final String TEST_VIEW = "my_view";

    @Rule
    public TestName testName = new TestName();

    @Mock
    private AmazonTimestreamQuery mockClient;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }

    @Before
    public void setUp()
            throws IOException
    {
        logger.info("{}: enter", testName.getMethodName());

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(AmazonS3.class);

        when(amazonS3.putObject(any()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                    }
                    return mock(PutObjectResult.class);
                });

        when(amazonS3.getObject(nullable(String.class), nullable(String.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                    }
                    when(mockObject.getObjectContent()).thenReturn(
                            new S3ObjectInputStream(
                                    new ByteArrayInputStream(byteHolder.getBytes()), null));
                    return mockObject;
                });

        schemaForRead = SchemaBuilder.newBuilder()
                .addField("measure_name", Types.MinorType.VARCHAR.getType())
                .addField("measure_value::double", Types.MinorType.FLOAT8.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("time", Types.MinorType.DATEMILLI.getType())
                .addField("hostname", Types.MinorType.VARCHAR.getType())
                .addField("region", Types.MinorType.VARCHAR.getType())
                .build();

        handler = new TimestreamRecordHandler(amazonS3, mockSecretsManager, mockAthena, mockClient, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        int numRowsGenerated = 1_000;
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResult mockResult = makeMockQueryResult(schemaForRead, numRowsGenerated);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResult>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.getQueryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        SpillLocation splitLoc = SpillLocation.newBuilder().setBucket(UUID.randomUUID().toString()).setKey(UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString()).setDirectory(true).build();

        Split.Builder splitBuilder = Split.newBuilder().setSpillLocation(splitLoc).setEncryptionKey(keyFactory.create());

        ReadRecordsRequest request = ReadRecordsRequest.newBuilder().setIdentity(IDENTITY).setCatalogName(DEFAULT_CATALOG).setQueryId("queryId-" + System.currentTimeMillis()).setTableName(TableName.newBuilder().setSchemaName(DEFAULT_SCHEMA).setTableName(TEST_TABLE)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schemaForRead)).setSplit(splitBuilder.build()).setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap))).setMaxBlockSize(100_000_000_000L).setMaxInlineBlockSize(100_000_000_000L).build();

        Message rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount());

        assertTrue(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount() > 0);

        //ensure we actually filtered something out
        assertTrue(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount() < numRowsGenerated);

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()), 0));
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResult mockResult = makeMockQueryResult(schemaForRead, 100_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResult>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.getQueryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        SpillLocation splitLoc = SpillLocation.newBuilder().setBucket(UUID.randomUUID().toString()).setKey(UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString()).setDirectory(true).build();

        Split.Builder splitBuilder = Split.newBuilder().setSpillLocation(splitLoc).setEncryptionKey(keyFactory.create());

        ReadRecordsRequest request = ReadRecordsRequest.newBuilder().setIdentity(IDENTITY).setCatalogName(DEFAULT_CATALOG).setQueryId("queryId-" + System.currentTimeMillis()).setTableName(TableName.newBuilder().setSchemaName(DEFAULT_SCHEMA).setTableName(TEST_TABLE)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schemaForRead)).setSplit(splitBuilder.build()).setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap))).setMaxBlockSize(1_500_000L).setMaxInlineBlockSize(0L).build();
        Message rawResponse = handler.doReadRecords(allocator, request);
        RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocksList().size());

        assertTrue(response.getRemoteBlocksList().size() > 1);

        int blockNum = 0;
        for (SpillLocation next : response.getRemoteBlocksList()) {
            SpillLocation spillLocation = (SpillLocation) next;
            try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), ProtobufMessageConverter.fromProtoSchema(allocator, response.getSchema()))) {

                logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                // assertTrue(++blockNum < response.getRemoteBlocksList().size() && block.getRowCount() > 10_000);

                logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                assertNotNull(BlockUtils.rowToString(block, 0));
            }
        }
    }

    @Test
    public void readRecordsView()
            throws Exception
    {
        logger.info("readRecordsView - enter");

        Schema schemaForReadView = SchemaBuilder.newBuilder()
                .addField("measure_name", Types.MinorType.VARCHAR.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("value", Types.MinorType.FLOAT8.getType())
                .addField("num_samples", Types.MinorType.BIGINT.getType())
                .addMetadata(VIEW_METADATA_FIELD, "select measure_name, az,sum(\"measure_value::double\") as value, count(*) as num_samples from \"" +
                        DEFAULT_SCHEMA + "\".\"" + TEST_TABLE + "\" group by measure_name, az")
                .build();

        String expectedQuery = "WITH t1 AS ( select measure_name, az,sum(\"measure_value::double\") as value, count(*) as num_samples from \"my_schema\".\"my_table\" group by measure_name, az )  SELECT measure_name, az, value, num_samples FROM t1 WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResult mockResult = makeMockQueryResult(schemaForReadView, 1_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResult>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.getQueryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        SpillLocation splitLoc = SpillLocation.newBuilder().setBucket(UUID.randomUUID().toString()).setKey(UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString()).setDirectory(true).build();

        Split split = Split.newBuilder().setSpillLocation(splitLoc).build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = ReadRecordsRequest.newBuilder().setIdentity(IDENTITY).setCatalogName("default").setQueryId("queryId-" + System.currentTimeMillis()).setTableName(TableName.newBuilder().setSchemaName(DEFAULT_SCHEMA).setTableName(TEST_VIEW)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schemaForReadView)).setSplit(split).setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap))).setMaxBlockSize(100_000_000_000L).setMaxInlineBlockSize(100_000_000_000L).build();

        Message rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readRecordsView: rows[{}]", ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount());

        for (int i = 0; i < ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount() && i < 10; i++) {
            logger.info("readRecordsView: {}", BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()), i));
        }

        logger.info("readRecordsView - exit");
    }

    @Test
    public void readRecordsTimeSeriesView()
            throws Exception
    {
        logger.info("readRecordsTimeSeriesView - enter");

        Schema schemaForReadView = SchemaBuilder.newBuilder()
                .addField("region", Types.MinorType.VARCHAR.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("hostname", Types.MinorType.VARCHAR.getType())
                .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addFloat8Field("measure_value::double")
                                .build())
                        .build())
                .addMetadata(VIEW_METADATA_FIELD, "select az, hostname, region,  CREATE_TIME_SERIES(time, measure_value::double) as cpu_utilization from \"" + DEFAULT_SCHEMA + "\".\"" + TEST_TABLE + "\" WHERE measure_name = 'cpu_utilization' GROUP BY measure_name, az, hostname, region")
                .build();

        String expectedQuery = "WITH t1 AS ( select az, hostname, region,  CREATE_TIME_SERIES(time, measure_value::double) as cpu_utilization from \"my_schema\".\"my_table\" WHERE measure_name = 'cpu_utilization' GROUP BY measure_name, az, hostname, region )  SELECT region, az, hostname, cpu_utilization FROM t1 WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResult mockResult = makeMockQueryResult(schemaForReadView, 1_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResult>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals("actual: " + request.getQueryString(), expectedQuery, request.getQueryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        SpillLocation splitLoc = SpillLocation.newBuilder().setBucket(UUID.randomUUID().toString()).setKey(UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString()).setDirectory(true).build();

        Split split = Split.newBuilder().setSpillLocation(splitLoc).build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = ReadRecordsRequest.newBuilder().setIdentity(IDENTITY).setCatalogName("default").setQueryId("queryId-" + System.currentTimeMillis()).setTableName(TableName.newBuilder().setSchemaName(DEFAULT_SCHEMA).setTableName(TEST_TABLE)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schemaForReadView)).setSplit(split).setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap))).setMaxBlockSize(100_000_000_000L).setMaxInlineBlockSize(100_000_000_000L).build();

        Message rawResponse = handler.doReadRecords(allocator, request);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readRecordsTimeSeriesView: rows[{}]", ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount());

        for (int i = 0; i < ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount() && i < 10; i++) {
            logger.info("readRecordsTimeSeriesView: {}", BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()), i));
        }

        logger.info("readRecordsTimeSeriesView - exit");
    }

    @Test
    public void doReadRecordsNoSpillValidateTimeStamp()
            throws Exception
    {

        int numRows = 10;
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a'))";

        QueryResult mockResult = makeMockQueryResult(schemaForRead, numRows, numRows, false);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResult>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.getQueryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a").build());

        SpillLocation splitLoc = SpillLocation.newBuilder().setBucket(UUID.randomUUID().toString()).setKey(UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString()).setDirectory(true).build();

        Split.Builder splitBuilder = Split.newBuilder().setSpillLocation(splitLoc).setEncryptionKey(keyFactory.create());

        ReadRecordsRequest request = ReadRecordsRequest.newBuilder().setIdentity(IDENTITY).setCatalogName(DEFAULT_CATALOG).setQueryId("queryId-" + System.currentTimeMillis()).setTableName(TableName.newBuilder().setSchemaName(DEFAULT_SCHEMA).setTableName(TEST_TABLE)).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schemaForRead)).setSplit(splitBuilder.build()).setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap))).setMaxBlockSize(100_000_000_000L).setMaxInlineBlockSize(100_000_000_000L).build();

        Message rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount());

        assertTrue(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount() > 0);

        Block block = ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords());
        FieldReader time = block.getFieldReader("time");
        for (int i = 0; i < ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount() && i < numRows; i++) {
            time.setPosition(i);
            assertTrue(time.readObject() instanceof LocalDateTime);
            assertEquals(TestUtils.startDate.plusDays(i).truncatedTo(ChronoUnit.MILLIS), time.readObject());
        }

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()), 0));
    }
}

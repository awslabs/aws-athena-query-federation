/*-
 * #%L
 * athena-tpcds
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
package com.amazonaws.athena.connectors.tpcds;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.protobuf.Message;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_NUMBER_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_SCALE_FACTOR_FIELD;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.SPLIT_TOTAL_NUMBER_FIELD;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TPCDSRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSRecordHandlerTest.class);

    private FederatedIdentity identity = FederatedIdentity.newBuilder().setArn("arn").setAccount("account").build();
    private List<ByteHolder> mockS3Storage;
    private TPCDSRecordHandler handler;
    private S3BlockSpillReader spillReader;
    private BlockAllocator allocator;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private Table table;
    private Schema schemaForRead;

    @Mock
    private AmazonS3 mockS3;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        for (Table next : Table.getBaseTables()) {
            if (next.getName().equals("customer")) {
                table = next;
            }
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (Column nextCol : table.getColumns()) {
            schemaBuilder.addField(TPCDSUtils.convertColumn(nextCol));
        }
        schemaForRead = schemaBuilder.build();

        mockS3Storage = new ArrayList<>();
        allocator = new BlockAllocatorImpl();
        handler = new TPCDSRecordHandler(mockS3, mockSecretsManager, mockAthena, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(mockS3, allocator);

        when(mockS3.putObject(any()))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    synchronized (mockS3Storage) {
                        InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                        ByteHolder byteHolder = new ByteHolder();
                        byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                        mockS3Storage.add(byteHolder);
                        return mock(PutObjectResult.class);
                    }
                });

        when(mockS3.getObject(nullable(String.class), nullable(String.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    synchronized (mockS3Storage) {
                        S3Object mockObject = mock(S3Object.class);
                        ByteHolder byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        when(mockObject.getObjectContent()).thenReturn(
                                new S3ObjectInputStream(
                                        new ByteArrayInputStream(byteHolder.getBytes()), null));
                        return mockObject;
                    }
                });
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        logger.info("doReadRecordsNoSpill: enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("c_customer_id", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                .add("AAAAAAAABAAAAAAA")
                .add("AAAAAAAACAAAAAAA")
                .add("AAAAAAAADAAAAAAA").build());

        ReadRecordsRequest request = ReadRecordsRequest.newBuilder()
            .setIdentity(identity)
            .setCatalogName("catalog")
            .setQueryId("queryId-" + System.currentTimeMillis())
            .setTableName(TableName.newBuilder().setSchemaName("tpcds1").setTableName(table.getName()).build())
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schemaForRead))
            .setSplit(Split.newBuilder().setSpillLocation(
                SpillLocation.newBuilder()
                                .setBucket(UUID.randomUUID().toString())
                                .setKey(UUID.randomUUID().toString() + '/' + UUID.randomUUID().toString())
                                .setDirectory(true)
                            .build()
                ).setEncryptionKey(keyFactory.create())
                .putProperties(SPLIT_NUMBER_FIELD, "0")
                .putProperties(SPLIT_TOTAL_NUMBER_FIELD, "1000")
                .putProperties(SPLIT_SCALE_FACTOR_FIELD, "1")
                .build())
            .setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap)))
            .setMaxBlockSize(100_000_000_000L)
            .setMaxInlineBlockSize(100_000_000_000L)
            .build();

        Message rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount());

        assertTrue(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount() == 3);
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()), 0));

        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("c_current_cdemo_sk", SortedRangeSet.of(
                Range.range(allocator, Types.MinorType.BIGINT.getType(), 100L, true, 100_000_000L, true)));

        ReadRecordsRequest request = ReadRecordsRequest.newBuilder()
            .setIdentity(identity)
            .setCatalogName("catalog")
            .setQueryId("queryId-" + System.currentTimeMillis())
            .setTableName(TableName.newBuilder().setSchemaName("tpcds1").setTableName(table.getName()).build())
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schemaForRead))
            .setSplit(Split.newBuilder().setSpillLocation(
                SpillLocation.newBuilder()
                                .setBucket(UUID.randomUUID().toString())
                                .setKey(UUID.randomUUID().toString() + '/' + UUID.randomUUID().toString())
                                .setDirectory(true)
                            .build()
                ).setEncryptionKey(keyFactory.create())
                .putProperties(SPLIT_NUMBER_FIELD, "0")
                .putProperties(SPLIT_TOTAL_NUMBER_FIELD, "1000")
                .putProperties(SPLIT_SCALE_FACTOR_FIELD, "1")
                .build())
            .setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(constraintsMap)))
            .setMaxBlockSize(1_500_000L)
            .setMaxInlineBlockSize(0)
            .build();

        Message rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

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
        logger.info("doReadRecordsSpill: exit");
    }

    @Test
    public void doReadRecordForTPCDSTIMETypeColumn()
            throws Exception
    {
        for (Table next : Table.getBaseTables()) {
            if (next.getName().equals("dbgen_version")) {
                table = next;
            }
        }
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (Column nextCol : table.getColumns()) {
            schemaBuilder.addField(TPCDSUtils.convertColumn(nextCol));
        }

        ReadRecordsRequest request = ReadRecordsRequest.newBuilder()
            .setIdentity(identity)
            .setCatalogName("catalog")
            .setQueryId("queryId-" + System.currentTimeMillis())
            .setTableName(TableName.newBuilder().setSchemaName("tpcds1").setTableName(table.getName()).build())
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schemaBuilder.build()))
            .setSplit(Split.newBuilder().setSpillLocation(
                SpillLocation.newBuilder()
                                .setBucket(UUID.randomUUID().toString())
                                .setKey(UUID.randomUUID().toString() + '/' + UUID.randomUUID().toString())
                                .setDirectory(true)
                            .build()
                ).setEncryptionKey(keyFactory.create())
                .putProperties(SPLIT_NUMBER_FIELD, "0")
                .putProperties(SPLIT_TOTAL_NUMBER_FIELD, "1000")
                .putProperties(SPLIT_SCALE_FACTOR_FIELD, "1")
                .build())
            .setMaxBlockSize(100_000_000_000L)
            .setMaxInlineBlockSize(100_000_000_000L)
            .build();

        Message rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

        logger.info("doReadRecordForTPCDSTIMETypeColumn: {}", BlockUtils.rowToString(ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()), 0));
        assertEquals(1, ProtobufMessageConverter.fromProtoBlock(allocator, response.getRecords()).getRowCount()); // TPCDS for `dbgen_version` always generates 1 record.

        logger.info("doReadRecordForTPCDSTIMETypeColumn: exit");
    }

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
}

package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.lambda.ProtoUtils;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocationVerifier;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.proto.request.PingRequest;
import com.amazonaws.athena.connector.lambda.proto.request.PingResponse;
import com.amazonaws.athena.connector.lambda.proto.request.TypeHeader;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(CompositeHandlerTest.class);

    private MetadataHandler mockMetadataHandler;
    private RecordHandler mockRecordHandler;
    private CompositeHandler compositeHandler;
    private BlockAllocatorImpl allocator;
    private ObjectMapper objectMapper;
    private Schema schemaForRead;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());

        allocator = new BlockAllocatorImpl();
        objectMapper = ObjectMapperFactory.create(allocator);
        mockMetadataHandler = mock(MetadataHandler.class);
        mockRecordHandler = mock(RecordHandler.class);

        schemaForRead = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .build();

        when(mockMetadataHandler.doGetTableLayout(nullable(BlockAllocatorImpl.class), nullable(GetTableLayoutRequest.class)))
                .thenReturn(GetTableLayoutResponse.newBuilder()
                    .setType("GetTableLayoutResponse")
                    .setCatalogName("catalog")
                    .setTableName(com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                        .setSchemaName("schema")
                        .setTableName("table")
                        .build()
                    ).setPartitions(ProtoUtils.toProtoBlock(BlockUtils.newBlock(allocator, "col1", Types.MinorType.BIGINT.getType(), 1L)))
                    .build());

        when(mockMetadataHandler.doListTables(nullable(BlockAllocatorImpl.class), nullable(ListTablesRequest.class)))
                .thenReturn(ListTablesResponse.newBuilder()
                    .setCatalogName("catalog")
                    .addTables(
                        com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                            .setSchemaName("schema")
                            .setTableName("table")
                            .build()
                    )
                    .build());

        when(mockMetadataHandler.doGetTable(nullable(BlockAllocatorImpl.class), nullable(GetTableRequest.class)))
                .thenReturn(GetTableResponse.newBuilder()
                    .setCatalogName("catalog")
                    .setTableName(
                        com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                            .setSchemaName("schema")
                            .setTableName("table")
                            .build()
                    ).setSchema(
                        ProtoUtils.toProtoSchemaBytes(SchemaBuilder.newBuilder().addStringField("col1").build())
                    )
                    .build()
                );

        when(mockMetadataHandler.doListSchemaNames(nullable(BlockAllocatorImpl.class), nullable(ListSchemasRequest.class)))
                .thenReturn(ListSchemasResponse.newBuilder()
                    .setCatalogName("catalog")
                    .addSchemas("schema1")
                    .build()
                );

        when(mockMetadataHandler.doGetSplits(nullable(BlockAllocatorImpl.class), nullable(GetSplitsRequest.class)))
                .thenReturn(GetSplitsResponse.newBuilder().setCatalogName("catalog").addSplits(com.amazonaws.athena.connector.lambda.proto.domain.Split.newBuilder().build()).build());

        when(mockMetadataHandler.doPing(nullable(PingRequest.class)))
                .thenReturn(PingResponse.newBuilder().setCatalogName("catalog").setQueryId("queryId").setSourceType("type").setCapabilities(23).setSerDeVersion(2).build());

        when(mockRecordHandler.doReadRecords(nullable(BlockAllocatorImpl.class), nullable(ReadRecordsRequest.class)))
                .thenReturn(ReadRecordsResponse.newBuilder()
                    .setCatalogName("catalog")
                    .setRecords(ProtoUtils.toProtoBlock(BlockUtils.newEmptyBlock(allocator, "col", new ArrowType.Int(32, true))))
                    .build());

        compositeHandler = new CompositeHandler(mockMetadataHandler, mockRecordHandler);
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecords()
            throws Exception
    {

        ReadRecordsRequest req = ReadRecordsRequest.newBuilder()
            .setIdentity(FederatedIdentity.newBuilder()
                .setArn("arn")
                .setAccount("account")
                .build())
            .setCatalogName("catalog")
            .setQueryId("queryId-" + System.currentTimeMillis())
            .setSchema(ProtoUtils.toProtoSchemaBytes(schemaForRead))
            .setSplit(
                com.amazonaws.athena.connector.lambda.proto.domain.Split.newBuilder()
                .setSpillLocation(
                    com.amazonaws.athena.connector.lambda.proto.domain.spill.SpillLocation.newBuilder()
                    .setBucket("athena-virtuoso-test")
                    .setKey("lambda-spill/" + UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString()) // String key = prefix + SEPARATOR + queryId + SEPARATOR + splitId;
                    .setDirectory(true)
                    .build())
                // TODO - does null encryption key serialize properly?
                .build()
            ).setConstraints(
                com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints.newBuilder()
                    .putAllSummary(new HashMap<>())
                    .build()
            ).setMaxBlockSize(100_000_000_000L) // 100GB don't expect this to spill
            .setMaxInlineBlockSize(100_000_000_000L)
            .build();
        TypeHeader typeHeader = TypeHeader.newBuilder().setType("ReadRecordsRequest").build();
        String inputJson = JsonFormat.printer().includingDefaultValueFields().print(req);
        compositeHandler.handleRequest(allocator, typeHeader, inputJson, new ByteArrayOutputStream());
        verify(mockRecordHandler, times(1))
                .doReadRecords(nullable(BlockAllocator.class), nullable(ReadRecordsRequest.class));
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        ListSchemasRequest req = ListSchemasRequest.newBuilder().setType("ListSchemasRequest").build();
        TypeHeader typeHeader = TypeHeader.newBuilder().setType("ListSchemasRequest").build();
        String inputJson = JsonFormat.printer().includingDefaultValueFields().print(req);
        compositeHandler.handleRequest(allocator, typeHeader, inputJson, new ByteArrayOutputStream());
        verify(mockMetadataHandler, times(1)).doListSchemaNames(nullable(BlockAllocatorImpl.class), nullable(ListSchemasRequest.class));
    }

    @Test
    public void doListTables()
            throws Exception
    {
        ListTablesRequest req = ListTablesRequest.newBuilder().setType("ListTablesRequest").build();
        TypeHeader typeHeader = TypeHeader.newBuilder().setType("ListTablesRequest").build();
        String inputJson = JsonFormat.printer().includingDefaultValueFields().print(req);
        compositeHandler.handleRequest(allocator, typeHeader, inputJson, new ByteArrayOutputStream());
        verify(mockMetadataHandler, times(1)).doListTables(nullable(BlockAllocatorImpl.class), nullable(ListTablesRequest.class));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        GetTableRequest req = GetTableRequest.newBuilder().setType("GetTableRequest").build();
        TypeHeader typeHeader = TypeHeader.newBuilder().setType("GetTableRequest").build();
        String inputJson = JsonFormat.printer().includingDefaultValueFields().print(req);
        compositeHandler.handleRequest(allocator, typeHeader, inputJson, new ByteArrayOutputStream());
        verify(mockMetadataHandler, times(1)).doGetTable(nullable(BlockAllocatorImpl.class), nullable(GetTableRequest.class));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        TypeHeader typeHeader = TypeHeader.newBuilder()
            .setType("GetTableLayoutRequest")
            .build();
        GetTableLayoutRequest request = GetTableLayoutRequest.newBuilder()
            .setType("GetTableLayoutRequest")
            .build();
        String inputJson = JsonFormat.printer().includingDefaultValueFields().print(request);

        compositeHandler.handleRequest(allocator, typeHeader, inputJson, new ByteArrayOutputStream());
        verify(mockMetadataHandler, times(1)).doGetTableLayout(nullable(BlockAllocatorImpl.class), nullable(GetTableLayoutRequest.class));
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        SpillLocationVerifier mockVerifier = mock(SpillLocationVerifier.class);
        doNothing().when(mockVerifier).checkBucketAuthZ(nullable(String.class));
        Whitebox.setInternalState(mockMetadataHandler, "verifier", mockVerifier);
        
        TypeHeader typeHeader = TypeHeader.newBuilder()
            .setType("GetSplitsRequest")
            .build();
        GetSplitsRequest request = GetSplitsRequest.newBuilder()
            .setType("GetSplitsRequest")
            .build();
        String inputJson = JsonFormat.printer().includingDefaultValueFields().print(request);

        compositeHandler.handleRequest(allocator, typeHeader, inputJson, new ByteArrayOutputStream());
        verify(mockMetadataHandler, times(1)).doGetSplits(nullable(BlockAllocatorImpl.class), nullable(GetSplitsRequest.class));
    }

    @Test
    public void doPing()
            throws Exception
    {
        TypeHeader typeHeader = TypeHeader.newBuilder()
            .setType("PingRequest")
            .build();
        PingRequest req = PingRequest.newBuilder()
            .setCatalogName("catalog")
            .setQueryId("queryId")
            .setType("PingRequest")
            .build();
        String inputJson = JsonFormat.printer().includingDefaultValueFields().print(req);
        compositeHandler.handleRequest(allocator, typeHeader, inputJson, new ByteArrayOutputStream());
        verify(mockMetadataHandler, times(1)).doPing(nullable(PingRequest.class));
    }
}

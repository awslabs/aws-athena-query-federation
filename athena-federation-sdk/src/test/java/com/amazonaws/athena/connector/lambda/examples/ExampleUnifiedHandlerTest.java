package com.amazonaws.athena.connector.lambda.examples;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExampleUnifiedHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandlerTest.class);

    private AmazonS3 mockS3;
    private ExampleMetadataHandler mockMetadataHandler;
    private ExampleRecordHandler mockRecordHandler;
    private ExampleUnifiedHandler unifiedHandler;
    private BlockAllocatorImpl allocator;
    private ObjectMapper objectMapper;
    private Schema schemaForRead;

    @Before
    public void setUp()
            throws Exception
    {
        allocator = new BlockAllocatorImpl();
        objectMapper = ObjectMapperFactory.create(allocator);
        mockS3 = mock(AmazonS3.class);
        mockMetadataHandler = mock(ExampleMetadataHandler.class);

        when(mockMetadataHandler.doGetTableLayout(any(BlockAllocatorImpl.class), any(GetTableLayoutRequest.class)))
                .thenReturn(new GetTableLayoutResponse("catalog",
                        new TableName("schema", "table"),
                        BlockUtils.newBlock(allocator, "col1", Types.MinorType.BIGINT.getType(), 1L),
                        new HashSet<>()));

        when(mockMetadataHandler.doListTables(any(BlockAllocatorImpl.class), any(ListTablesRequest.class)))
                .thenReturn(new ListTablesResponse("catalog",
                        Collections.singletonList(new TableName("schema", "table"))));

        when(mockMetadataHandler.doGetTable(any(BlockAllocatorImpl.class), any(GetTableRequest.class)))
                .thenReturn(new GetTableResponse("catalog",
                        new TableName("schema", "table"),
                        SchemaBuilder.newBuilder().addStringField("col1").build()));

        when(mockMetadataHandler.doListSchemaNames(any(BlockAllocatorImpl.class), any(ListSchemasRequest.class)))
                .thenReturn(new ListSchemasResponse("catalog", Collections.singleton("schema1")));

        when(mockMetadataHandler.doGetSplits(any(BlockAllocatorImpl.class), any(GetSplitsRequest.class)))
                .thenReturn(new GetSplitsResponse("catalog", Split.newBuilder(null, null).build()));

        mockRecordHandler = mock(ExampleRecordHandler.class);
        unifiedHandler = new ExampleUnifiedHandler(mockS3, mockMetadataHandler, mockRecordHandler);
        schemaForRead = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .build();
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void doReadRecords()
            throws Exception
    {
        logger.info("doReadRecords - enter");
        ReadRecordsRequest req = new ReadRecordsRequest(IdentityUtil.fakeIdentity(),
                "catalog",
                "queryId-" + System.currentTimeMillis(),
                new TableName("schema", "table"),
                schemaForRead,
                Split.newBuilder(S3SpillLocation.newBuilder()
                        .withBucket("athena-virtuoso-test")
                        .withPrefix("lambda-spill")
                        .withQueryId(UUID.randomUUID().toString())
                        .withSplitId(UUID.randomUUID().toString())
                        .withIsDirectory(true)
                        .build(), null).build(),
                new Constraints(new HashMap<>()),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );
        unifiedHandler.handleRequest(allocator, req, new ByteArrayOutputStream(), objectMapper);
        verify(mockRecordHandler, times(1))
                .readWithConstraint(any(ConstraintEvaluator.class), any(BlockSpiller.class), any(ReadRecordsRequest.class));
        logger.info("readRecords - exit");
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        logger.info("doListSchemas - enter");
        ListSchemasRequest req = mock(ListSchemasRequest.class);
        when(req.getRequestType()).thenReturn(MetadataRequestType.LIST_SCHEMAS);
        unifiedHandler.handleRequest(allocator, req, new ByteArrayOutputStream(), objectMapper);
        verify(mockMetadataHandler, times(1)).doListSchemaNames(any(BlockAllocatorImpl.class), any(ListSchemasRequest.class));
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
            throws Exception
    {
        logger.info("doListTables - enter");
        ListTablesRequest req = mock(ListTablesRequest.class);
        when(req.getRequestType()).thenReturn(MetadataRequestType.LIST_TABLES);
        unifiedHandler.handleRequest(allocator, req, new ByteArrayOutputStream(), objectMapper);
        verify(mockMetadataHandler, times(1)).doListTables(any(BlockAllocatorImpl.class), any(ListTablesRequest.class));
        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        logger.info("doGetTable - enter");
        GetTableRequest req = mock(GetTableRequest.class);
        when(req.getRequestType()).thenReturn(MetadataRequestType.GET_TABLE);
        unifiedHandler.handleRequest(allocator, req, new ByteArrayOutputStream(), objectMapper);
        verify(mockMetadataHandler, times(1)).doGetTable(any(BlockAllocatorImpl.class), any(GetTableRequest.class));
        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");
        GetTableLayoutRequest req = mock(GetTableLayoutRequest.class);
        when(req.getRequestType()).thenReturn(MetadataRequestType.GET_TABLE_LAYOUT);
        unifiedHandler.handleRequest(allocator, req, new ByteArrayOutputStream(), objectMapper);
        verify(mockMetadataHandler, times(1)).doGetTableLayout(any(BlockAllocatorImpl.class), any(GetTableLayoutRequest.class));
        logger.info("doGetTableLayout - exit");
    }

    @Test
    public void doGetSplits()
            throws Exception
    {
        logger.info("doGetSplits - enter");
        GetSplitsRequest req = mock(GetSplitsRequest.class);
        when(req.getRequestType()).thenReturn(MetadataRequestType.GET_SPLITS);
        unifiedHandler.handleRequest(allocator, req, new ByteArrayOutputStream(), objectMapper);
        verify(mockMetadataHandler, times(1)).doGetSplits(any(BlockAllocatorImpl.class), any(GetSplitsRequest.class));
        logger.info("doGetSplits - exit");
    }
}
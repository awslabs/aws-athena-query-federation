/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.protobuf;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.proto.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.proto.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.proto.request.PingRequest;
import com.amazonaws.athena.connector.lambda.proto.request.PingResponse;
import com.amazonaws.athena.connector.lambda.proto.request.TypeHeader;
import com.amazonaws.athena.connector.lambda.proto.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.athena.connector.lambda.utils.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.*;


/**
 * Class takes the Jackson SerDe Json test files and ensures we can successfully serialize to and from them using the Protobuf messages.
 */
public class ProtobufSerializationCompatibilityTest
{
    private BlockAllocator allocator = new BlockAllocatorImpl("test-allocator-id");
    private TestUtils utils = new TestUtils();
    private static Logger logger = LoggerFactory.getLogger(ProtobufSerializationCompatibilityTest.class);
    private static FederatedIdentity TEST_IDENTITY = FederatedIdentity.newBuilder()
        .setId("UNKNOWN") // this is a deprecated field
        .setArn("testArn")
        .setAccount("0123456789")
        .setPrincipal("UNKNOWN")
        .build();
    private static TableName TEST_TABLE_NAME = TableName.newBuilder()
        .setSchemaName("test-schema")
        .setTableName("test-table")
        .build();
    private static String TEST_QUERY_ID = "test-query-id";
    private static String TEST_CATALOG = "test-catalog";
    private static String TEST_SOURCE_TYPE = "test-source-type";
    // private static String BYTE_STRING_PARTITIONS_SCHEMA = "7AEAABAAAAAAAAoADgAGAA0ACAAKAAAAAAADABAAAAAAAQoADAAAAAgABAAKAAAACAAAAAgAAAAAAAAABwAAAHABAAAkAQAA7AAAALQAAAB0AAAAPAAAAAQAAAC+/v//FAAAABQAAAAUAAAAAAADARQAAAAAAAAAAAAAAJb///8AAAIABAAAAGNvbDUAAAAA8v7//xQAAAAUAAAAFAAAAAAAAwEUAAAAAAAAAAAAAADK////AAACAAQAAABjb2w0AAAAACb///8UAAAAFAAAABwAAAAAAAMBHAAAAAAAAAAAAAAAAAAGAAgABgAGAAAAAAACAAQAAABjb2wzAAAAAGL///8UAAAAFAAAABgAAAAAAAUBFAAAAAAAAAAAAAAABAAEAAQAAAAEAAAAY29sMgAAAACW////FAAAABQAAAAUAAAAAAACARgAAAAAAAAAAAAAAIT///8AAAABIAAAAAMAAABkYXkAyv///xQAAAAUAAAAFAAAAAAAAgEYAAAAAAAAAAAAAAC4////AAAAASAAAAAFAAAAbW9udGgAEgAYABQAEwASAAwAAAAIAAQAEgAAABQAAAAUAAAAHAAAAAAAAgEgAAAAAAAAAAAAAAAIAAwACAAHAAgAAAAAAAABIAAAAAQAAAB5ZWFyAAAAAA==";
    // private static String BYTE_STRING_PARTITIONS_RECORDS = "vAEAABQAAAAAAAAADAAWAA4AFQAQAAQADAAAANABAAAAAAAAAAADABAAAAAAAwoAGAAMAAgABAAKAAAAFAAAAAgBAAAKAAAAAAAAAAAAAAAPAAAAAAAAAAAAAAACAAAAAAAAAAgAAAAAAAAAKAAAAAAAAAAwAAAAAAAAAAIAAAAAAAAAOAAAAAAAAAAoAAAAAAAAAGAAAAAAAAAAAgAAAAAAAABoAAAAAAAAACgAAAAAAAAAkAAAAAAAAAACAAAAAAAAAJgAAAAAAAAALAAAAAAAAADIAAAAAAAAAAAAAAAAAAAAyAAAAAAAAAACAAAAAAAAANAAAAAAAAAAUAAAAAAAAAAgAQAAAAAAAAIAAAAAAAAAKAEAAAAAAABQAAAAAAAAAHgBAAAAAAAAAgAAAAAAAACAAQAAAAAAAFAAAAAAAAAAAAAAAAcAAAAKAAAAAAAAAAAAAAAAAAAACgAAAAAAAAAAAAAAAAAAAAoAAAAAAAAAAAAAAAAAAAAKAAAAAAAAAAoAAAAAAAAACgAAAAAAAAAKAAAAAAAAAAoAAAAAAAAACgAAAAAAAAAKAAAAAAAAAAoAAAAAAAAAAAAAAP8DAAAAAAAA4AcAAOEHAADiBwAA4wcAAOQHAADlBwAA5gcAAOcHAADoBwAA6QcAAP8DAAAAAAAAAQAAAAIAAAADAAAABAAAAAUAAAAGAAAABwAAAAgAAAAJAAAACgAAAP8DAAAAAAAAAQAAAAIAAAADAAAABAAAAAUAAAAGAAAABwAAAAgAAAAJAAAACgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    // private static String CONSTRAINTS_BLOCK_SCHEMA = "lAAAABAAAAAAAAoADgAGAA0ACAAKAAAAAAADABAAAAAAAQoADAAAAAgABAAKAAAACAAAAAgAAAAAAAAAAQAAABgAAAAAABIAGAAUABMAEgAMAAAACAAEABIAAAAUAAAAFAAAABwAAAAAAAMBHAAAAAAAAAAAAAAAAAAGAAgABgAGAAAAAAACAAQAAABjb2wxAAAAAAAAAAA=";
    // private static String CONSTRAINTS_BLOCK_RECORDS = "jAAAABQAAAAAAAAADAAWAA4AFQAQAAQADAAAABAAAAAAAAAAAAADABAAAAAAAwoAGAAMAAgABAAKAAAAFAAAADgAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAABAAAAAAAAAAgAAAAAAAAACAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAACamZmZmZnxPw==";
    private static String TEST_CONTINUATION_TOKEN = "test-continuation-token";

    private static Schema SCHEMA;
    // private static ByteString SCHEMA_EXPECTED_BYTE_STRING;
    private static Map<String, ValueSet> constraintsMap;
    private static Constraints CONSTRAINTS;
    private static Block PARTITIONS;
    String yearCol = "year";
    String monthCol = "month";
    String dayCol = "day";

    @Before
    public void setup() {
        // taken from now-deleted GetSplitsRequestSerDeTest.java
        SCHEMA = SchemaBuilder.newBuilder()
                .addField(yearCol, new ArrowType.Int(32, true))
                .addField(monthCol, new ArrowType.Int(32, true))
                .addField(dayCol, new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .addField("col3", Types.MinorType.FLOAT8.getType())
                .addField("col4", Types.MinorType.FLOAT8.getType())
                .addField("col5", Types.MinorType.FLOAT8.getType())
                .build();
        // SCHEMA_EXPECTED_BYTE_STRING = ProtobufMessageConverter.toProtoSchemaBytes(SCHEMA);

        constraintsMap = new HashMap<>();
        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), -10000D)), false));
        constraintsMap.put("col4", EquatableValueSet.newBuilder(allocator, Types.MinorType.FLOAT8.getType(), false, true).add(1.1D).build());
        constraintsMap.put("col5", new AllOrNoneValueSet(Types.MinorType.FLOAT8.getType(), false, true));
        CONSTRAINTS = new Constraints(constraintsMap);

        PARTITIONS = allocator.createBlock(SCHEMA);
        setPartitionValuesForBlock(PARTITIONS);
    }



    private void setPartitionValuesForBlock(Block block)
    {
        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(block.getFieldVector(yearCol), i, 2016 + i);
            BlockUtils.setValue(block.getFieldVector(monthCol), i, (i % 12) + 1);
            BlockUtils.setValue(block.getFieldVector(dayCol), i, (i % 28) + 1);
        }
        block.setRowCount(num_partitions);
    }

    @Test
    public void testPingRequest() throws IOException {
        String pingRequestJson = readFileAsJsonString("serde", "PingRequest.json");
        assertTypeHeaderParsesCorrectly(pingRequestJson, "PingRequest");
        PingRequest pingRequestFromJson = (PingRequest) ProtobufSerDe.buildFromJson(pingRequestJson, PingRequest.newBuilder());
        PingRequest pingRequestConstructed = PingRequest.newBuilder()
            .setType("PingRequest")
            .setCatalogName(TEST_CATALOG)
            .setQueryId(TEST_QUERY_ID)
            .setIdentity(TEST_IDENTITY)
            .build();
        assertEquals(pingRequestFromJson, pingRequestConstructed);
        logger.info(ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(pingRequestConstructed));

        String pingRequestConstructedToJson = ProtobufSerDe.writeMessageToJson(pingRequestConstructed);
        assertJsonEquivalentIgnoringWhitespace(pingRequestJson, pingRequestConstructedToJson);
    }

    @Test
    public void testPingResponse() throws IOException {
        String pingResponseJson = readFileAsJsonString("serde", "PingResponse.json");
        assertTypeHeaderParsesCorrectly(pingResponseJson, "PingResponse");

        PingResponse pingResponseFromJson = (PingResponse) ProtobufSerDe.buildFromJson(pingResponseJson, PingResponse.newBuilder());
        PingResponse pingResponseConstructed = PingResponse.newBuilder().setType("PingResponse").setCapabilities(23).setSerDeVersion(2).setCatalogName(TEST_CATALOG).setQueryId(TEST_QUERY_ID).setSourceType(TEST_SOURCE_TYPE).build();
        assertEquals(pingResponseFromJson, pingResponseConstructed);

        String pingResponseConstructedToJson = ProtobufSerDe.writeMessageToJson(pingResponseConstructed);
        assertJsonEquivalentIgnoringWhitespace(pingResponseJson, pingResponseConstructedToJson);
        
    }

    @Test
    public void testGetSplitsRequest() throws IOException {
        String getSplitsRequestJson = readFileAsJsonString("serde/v2", "GetSplitsRequest.json");
        assertTypeHeaderParsesCorrectly(getSplitsRequestJson, "GetSplitsRequest");
        GetSplitsRequest getSplitsRequestFromJson = (GetSplitsRequest) ProtobufSerDe.buildFromJson(getSplitsRequestJson, GetSplitsRequest.newBuilder());
        GetSplitsRequest getSplitsRequestConstructed = GetSplitsRequest.newBuilder()
            .setType("GetSplitsRequest")
            .setIdentity(TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG)
            .setTableName(TEST_TABLE_NAME)
            .setPartitions(ProtobufMessageConverter.toProtoBlock(PARTITIONS))
            .addAllPartitionColumns(ImmutableList.of("year", "month", "day"))
            .setConstraints(ProtobufMessageConverter.toProtoConstraints(CONSTRAINTS))
            .setContinuationToken(TEST_CONTINUATION_TOKEN)
            .build();
        assertEquals(getSplitsRequestFromJson, getSplitsRequestConstructed);
        
        String getSplitsRequestConstructedToJson = ProtobufSerDe.writeMessageToJson(getSplitsRequestConstructed);
        assertJsonEquivalentIgnoringWhitespace(getSplitsRequestJson, getSplitsRequestConstructedToJson);
    }

    @Test
    public void testGetSplitsResponse() throws IOException {
        String getSplitsResponseJson = readFileAsJsonString("serde/v2", "GetSplitsResponse.json");
        assertTypeHeaderParsesCorrectly(getSplitsResponseJson, "GetSplitsResponse");
        GetSplitsResponse getSplitsResponseFromJson = (GetSplitsResponse) ProtobufSerDe.buildFromJson(getSplitsResponseJson, GetSplitsResponse.newBuilder());
        GetSplitsResponse getSplitsResponseConstructed = GetSplitsResponse.newBuilder()
            .setType("GetSplitsResponse")
            .setCatalogName(TEST_CATALOG)
            .addAllSplits(
                com.google.common.collect.ImmutableList.of(
                    Split.newBuilder()
                        .setSpillLocation(
                            SpillLocation.newBuilder()
                                .setType("S3SpillLocation")
                                .setBucket("athena-virtuoso-test")
                                .setKey(ProtobufUtils.buildS3SpillLocationKey("lambda-spill", "test-query-id", "test-split-id-1"))
                                .setDirectory(true)
                                .build()
                        )
                        .setEncryptionKey(
                            EncryptionKey.newBuilder()
                            .setKey(ByteString.copyFrom("test-key-1".getBytes()))
                            .setNonce(ByteString.copyFrom("test-nonce-1".getBytes()))
                            .build()
                        )
                        .putAllProperties(
                            com.google.common.collect.ImmutableMap.of(
                                "month", "11",
                                "year", "2017",
                                "day", "1"
                            )
                        )
                    .build(),
                    Split.newBuilder()
                        .setSpillLocation(
                            SpillLocation.newBuilder()
                                .setType("S3SpillLocation")
                                .setBucket("athena-virtuoso-test")
                                .setKey(ProtobufUtils.buildS3SpillLocationKey("lambda-spill", "test-query-id", "test-split-id-2"))
                                .setDirectory(true)
                                .build()
                        )
                        .setEncryptionKey(
                            EncryptionKey.newBuilder()
                            .setKey(ByteString.copyFrom("test-key-2".getBytes()))
                            .setNonce(ByteString.copyFrom("test-nonce-2".getBytes()))
                            .build()
                        )
                        .putAllProperties(
                            com.google.common.collect.ImmutableMap.of(
                                "month", "11",
                                "year", "2017",
                                "day", "2"
                            )
                        )
                    .build()
                )
            )
            .setContinuationToken(TEST_CONTINUATION_TOKEN)
            .build();
            assertEquals(getSplitsResponseFromJson, getSplitsResponseConstructed);

            String getSplitsResponseConstructedToJson = ProtobufSerDe.writeMessageToJson(getSplitsResponseConstructed);
            assertJsonEquivalentIgnoringWhitespace(getSplitsResponseJson, getSplitsResponseConstructedToJson);
    }

    @Test
    public void testGetTableLayoutRequest() throws IOException {
        String getTableLayoutRequestJson = readFileAsJsonString("serde/v2", "GetTableLayoutRequest.json");
        assertTypeHeaderParsesCorrectly(getTableLayoutRequestJson, "GetTableLayoutRequest");
        GetTableLayoutRequest getTableLayoutRequestFromJson = (GetTableLayoutRequest) ProtobufSerDe.buildFromJson(getTableLayoutRequestJson, GetTableLayoutRequest.newBuilder());
        GetTableLayoutRequest getTableLayoutRequestConstructed = GetTableLayoutRequest.newBuilder()
            .setType("GetTableLayoutRequest")
            .setIdentity(TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG)
            .setTableName(TEST_TABLE_NAME)
            .setConstraints(ProtobufMessageConverter.toProtoConstraints(CONSTRAINTS))
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(SCHEMA))
            .addAllPartitionColumns(ImmutableList.of("month", "year", "day"))
            .build();
        assertEquals(getTableLayoutRequestFromJson, getTableLayoutRequestConstructed);
        logger.info(ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(getTableLayoutRequestConstructed));

        String getTableLayoutRequestConstructedToJson = ProtobufSerDe.writeMessageToJson(getTableLayoutRequestConstructed);
        assertJsonEquivalentIgnoringWhitespace(getTableLayoutRequestJson, getTableLayoutRequestConstructedToJson);
    }

    @Test
    public void testGetTableLayoutResponse() throws IOException {
        Schema schema = getGetTableRequestsSchema();
        Block partitions = allocator.createBlock(schema);
        setPartitionValuesForBlock(partitions);
        String getTableLayoutResponseJson = readFileAsJsonString("serde/v2", "GetTableLayoutResponse.json");
        assertTypeHeaderParsesCorrectly(getTableLayoutResponseJson, "GetTableLayoutResponse");
        GetTableLayoutResponse getTableLayoutResponseFromJson = (GetTableLayoutResponse) ProtobufSerDe.buildFromJson(getTableLayoutResponseJson, GetTableLayoutResponse.newBuilder());
        GetTableLayoutResponse getTableLayoutResponseConstructed = GetTableLayoutResponse.newBuilder()
            .setType("GetTableLayoutResponse")
            .setCatalogName(TEST_CATALOG)
            .setTableName(TEST_TABLE_NAME)
            .setPartitions(ProtobufMessageConverter.toProtoBlock(partitions))
            .build();
        assertEquals(getTableLayoutResponseFromJson, getTableLayoutResponseConstructed);
        logger.info(ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(getTableLayoutResponseConstructed));

        String getTableLayoutResponseConstructedToJson = ProtobufSerDe.writeMessageToJson(getTableLayoutResponseConstructed);
        assertJsonEquivalentIgnoringWhitespace(getTableLayoutResponseJson, getTableLayoutResponseConstructedToJson);
    }

    @Test
    public void testGetTableRequest() throws IOException {
        String getTableRequestJson = readFileAsJsonString("serde/v2", "GetTableRequest.json");
        assertTypeHeaderParsesCorrectly(getTableRequestJson, "GetTableRequest");
        GetTableRequest getTableRequestFromJson = (GetTableRequest) ProtobufSerDe.buildFromJson(getTableRequestJson, GetTableRequest.newBuilder());
        GetTableRequest getTableRequestConstructed = GetTableRequest.newBuilder()
            .setType("GetTableRequest")
            .setIdentity(TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG)
            .setTableName(TEST_TABLE_NAME)
            .build();
        assertEquals(getTableRequestFromJson, getTableRequestConstructed);
        logger.info(ProtobufSerDe.PROTOBUF_JSON_PRINTER.print(getTableRequestConstructed));

        String getTableRequestConstructedToJson = ProtobufSerDe.writeMessageToJson(getTableRequestConstructed);
        assertJsonEquivalentIgnoringWhitespace(getTableRequestJson, getTableRequestConstructedToJson);
    }

    @Test
    public void testGetTableResponse() throws IOException {
        Schema schema = getGetTableRequestsSchema();
        String getTableResponseJson = readFileAsJsonString("serde/v2", "GetTableResponse.json");
        assertTypeHeaderParsesCorrectly(getTableResponseJson, "GetTableResponse");
        GetTableResponse getTableResponseFromJson = (GetTableResponse) ProtobufSerDe.buildFromJson(getTableResponseJson, GetTableResponse.newBuilder());
        GetTableResponse getTableResponseConstructed = GetTableResponse.newBuilder()
            .setType("GetTableResponse")
            .setCatalogName(TEST_CATALOG)
            .setTableName(TEST_TABLE_NAME)
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schema))
            .addAllPartitionColumns(ImmutableList.of("year", "month", "day"))
            .build();
        assertEquals(getTableResponseFromJson,  getTableResponseConstructed);

        String getTableResponseConstructedToJson = ProtobufSerDe.writeMessageToJson(getTableResponseConstructed);
        assertJsonEquivalentIgnoringWhitespace(getTableResponseJson, getTableResponseConstructedToJson);
    }

    @Test
    public void testListSchemasRequest() throws IOException {
        String listSchemasRequestJson = readFileAsJsonString("serde/v2", "ListSchemasRequest.json");
        assertTypeHeaderParsesCorrectly(listSchemasRequestJson, "ListSchemasRequest");
        ListSchemasRequest listSchemasRequestFromJson = (ListSchemasRequest) ProtobufSerDe.buildFromJson(listSchemasRequestJson, ListSchemasRequest.newBuilder());
        ListSchemasRequest listSchemasRequestConstructed = ListSchemasRequest.newBuilder()
            .setType("ListSchemasRequest")
            .setIdentity(TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG)
            .build();
        assertEquals(listSchemasRequestFromJson, listSchemasRequestConstructed);

        String listSchemasRequestConstructedToJson = ProtobufSerDe.writeMessageToJson(listSchemasRequestConstructed);
        assertJsonEquivalentIgnoringWhitespace(listSchemasRequestJson, listSchemasRequestConstructedToJson);
    }

    @Test
    public void testListSchemasResponse() throws IOException {
        String listSchemasResponseJson = readFileAsJsonString("serde/v2", "ListSchemasResponse.json");
        assertTypeHeaderParsesCorrectly(listSchemasResponseJson, "ListSchemasResponse");
        ListSchemasResponse listSchemasResponseFromJson = (ListSchemasResponse) ProtobufSerDe.buildFromJson(listSchemasResponseJson, ListSchemasResponse.newBuilder());
        ListSchemasResponse listSchemasResponseConstructed = ListSchemasResponse.newBuilder()
            .setType("ListSchemasResponse")
            .setCatalogName(TEST_CATALOG)
            .addAllSchemas(ImmutableList.of("schema1", "schema2"))
            .build();
        assertEquals(listSchemasResponseFromJson, listSchemasResponseConstructed);

        String listSchemasResponseConstructedToJson = ProtobufSerDe.writeMessageToJson(listSchemasResponseConstructed);
        assertJsonEquivalentIgnoringWhitespace(listSchemasResponseJson, listSchemasResponseConstructedToJson);
        
    }

    @Test
    public void testListTablesRequest() throws IOException {
        String listTablesRequestJson = readFileAsJsonString("serde/v2", "ListTablesRequest.json");
        assertTypeHeaderParsesCorrectly(listTablesRequestJson, "ListTablesRequest");
        ListTablesRequest listTablesRequestFromJson = (ListTablesRequest) ProtobufSerDe.buildFromJson(listTablesRequestJson, ListTablesRequest.newBuilder());
        ListTablesRequest listTablesRequestConstructed = ListTablesRequest.newBuilder()
            .setType("ListTablesRequest")
            .setIdentity(TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG)
            .setSchemaName("test-schema")
            .setNextToken("table4")
            .setPageSize(25)
            .build();
        assertEquals(listTablesRequestFromJson, listTablesRequestConstructed);

        String listTablesRequestConstructedToJson = ProtobufSerDe.writeMessageToJson(listTablesRequestConstructed);
        assertJsonEquivalentIgnoringWhitespace(listTablesRequestJson, listTablesRequestConstructedToJson);
    }

    @Test
    public void testListTablesResponse() throws IOException {
        String listTablesResponseJson = readFileAsJsonString("serde/v2", "ListTablesResponse.json");
        assertTypeHeaderParsesCorrectly(listTablesResponseJson, "ListTablesResponse");
        ListTablesResponse listTablesResponseFromJson = (ListTablesResponse) ProtobufSerDe.buildFromJson(listTablesResponseJson, ListTablesResponse.newBuilder());
        ListTablesResponse listTablesResponseConstructed = ListTablesResponse.newBuilder()
            .setType("ListTablesResponse")
            .setCatalogName(TEST_CATALOG)
            .setNextToken("table3")
            .addAllTables(ImmutableList.of(
                TableName.newBuilder().setSchemaName("schema1").setTableName("table1").build(),
                TableName.newBuilder().setSchemaName("schema1").setTableName("table2").build()
            ))
            .build();
        assertEquals(listTablesResponseFromJson, listTablesResponseConstructed);

        String listTablesResponseConstructedToJson = ProtobufSerDe.writeMessageToJson(listTablesResponseConstructed);
        assertJsonEquivalentIgnoringWhitespace(listTablesResponseJson, listTablesResponseConstructedToJson);
    }

    @Test
    public void testReadRecordsRequest() throws IOException {
        String readRecordsRequestJson = readFileAsJsonString("serde/v2", "ReadRecordsRequest.json");
        assertTypeHeaderParsesCorrectly(readRecordsRequestJson, "ReadRecordsRequest");
        ReadRecordsRequest readRecordsRequestFromJson = (ReadRecordsRequest) ProtobufSerDe.buildFromJson(readRecordsRequestJson, ReadRecordsRequest.newBuilder());
        ReadRecordsRequest readRecordsRequestConstructed = ReadRecordsRequest.newBuilder()
            .setType("ReadRecordsRequest")
            .setIdentity(TEST_IDENTITY)
            .setQueryId(TEST_QUERY_ID)
            .setCatalogName(TEST_CATALOG)
            .setTableName(TEST_TABLE_NAME)
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(SCHEMA))
            .setSplit(
                Split.newBuilder()
                        .setSpillLocation(
                            SpillLocation.newBuilder()
                                .setType("S3SpillLocation")
                                .setBucket("athena-virtuoso-test")
                                .setKey(ProtobufUtils.buildS3SpillLocationKey("lambda-spill", "test-query-id", "test-split-id"))
                                .setDirectory(true)
                                .build()
                        )
                        .setEncryptionKey(
                            EncryptionKey.newBuilder()
                            .setKey(ByteString.copyFrom("test-key".getBytes()))
                            .setNonce(ByteString.copyFrom("test-nonce".getBytes()))
                            .build()
                        )
                        .putAllProperties(
                            com.google.common.collect.ImmutableMap.of(
                                "month", "11",
                                "year", "2017",
                                "day", "1"
                            )
                        )
            )
            .setConstraints(ProtobufMessageConverter.toProtoConstraints(CONSTRAINTS))
            .setMaxBlockSize(100000000000L)
            .setMaxInlineBlockSize(100000000000L)
            .build();
        assertEquals(readRecordsRequestFromJson,  readRecordsRequestConstructed);

        String readRecordsRequestConstructedToJson = ProtobufSerDe.writeMessageToJson(readRecordsRequestConstructed);
        assertJsonEquivalentIgnoringWhitespace(readRecordsRequestJson, readRecordsRequestConstructedToJson);
    }

    @Test
    public void testReadRecordsResponse() throws IOException {
        Schema schema = SchemaBuilder.newBuilder()
            .addField(yearCol, new ArrowType.Int(32, true))
            .addField(monthCol, new ArrowType.Int(32, true))
            .addField(dayCol, new ArrowType.Int(32, true))
            .build();
        Block records = allocator.createBlock(schema);
        setPartitionValuesForBlock(records);
        String readRecordsResponseJson = readFileAsJsonString("serde/v2", "ReadRecordsResponse.json");
        assertTypeHeaderParsesCorrectly(readRecordsResponseJson, "ReadRecordsResponse");
        ReadRecordsResponse readRecordsRequestFromJson = (ReadRecordsResponse) ProtobufSerDe.buildFromJson(readRecordsResponseJson, ReadRecordsResponse.newBuilder());
        ReadRecordsResponse readRecordsResponseConstructed = ReadRecordsResponse.newBuilder()
            .setType("ReadRecordsResponse")
            .setCatalogName(TEST_CATALOG)
            .setRecords(ProtobufMessageConverter.toProtoBlock(records))
            .build();
        assertEquals(readRecordsRequestFromJson,  readRecordsResponseConstructed);

        String readRecordsResponseConstructedToJson = ProtobufSerDe.writeMessageToJson(readRecordsResponseConstructed);
        assertJsonEquivalentIgnoringWhitespace(readRecordsResponseJson, readRecordsResponseConstructedToJson);        
    }

    @Test
    public void testRemoteReadRecordsResponse() throws IOException {
        Schema schema = SchemaBuilder.newBuilder()
            .addField(yearCol, new ArrowType.Int(32, true))
            .addField(monthCol, new ArrowType.Int(32, true))
            .addField(dayCol, new ArrowType.Int(32, true))
            .build();
        String remoteReadRecordsResponseJson = readFileAsJsonString("serde/v2", "RemoteReadRecordsResponse.json");
        assertTypeHeaderParsesCorrectly(remoteReadRecordsResponseJson, "RemoteReadRecordsResponse");
        RemoteReadRecordsResponse remoteReadRecordsResponseFromJson = (RemoteReadRecordsResponse) ProtobufSerDe.buildFromJson(remoteReadRecordsResponseJson, RemoteReadRecordsResponse.newBuilder());
        RemoteReadRecordsResponse remoteReadRecordsResponseConstructed = RemoteReadRecordsResponse.newBuilder()
            .setType("RemoteReadRecordsResponse")
            .setCatalogName(TEST_CATALOG)
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schema))
            .addAllRemoteBlocks(
                ImmutableList.of(
                    SpillLocation.newBuilder()
                        .setType("S3SpillLocation")
                        .setBucket("athena-virtuoso-test")
                        .setKey(ProtobufUtils.buildS3SpillLocationKey("lambda-spill", "test-query-id", "test-split-id-1"))
                        .setDirectory(true)
                        .build(),
                    SpillLocation.newBuilder()
                        .setType("S3SpillLocation")
                        .setBucket("athena-virtuoso-test")
                        .setKey(ProtobufUtils.buildS3SpillLocationKey("lambda-spill", "test-query-id", "test-split-id-2"))
                        .setDirectory(true)
                        .build()
                )
            )
            .setEncryptionKey(EncryptionKey.newBuilder()
                .setKey(ByteString.copyFrom("test-key".getBytes()))
                .setNonce(ByteString.copyFrom("test-nonce".getBytes()))
                .build()
            )
            .build();
        assertEquals(remoteReadRecordsResponseFromJson,  remoteReadRecordsResponseConstructed);

        String remoteReadRecordsResponseConstructedToJson = ProtobufSerDe.writeMessageToJson(remoteReadRecordsResponseConstructed);
        assertJsonEquivalentIgnoringWhitespace(remoteReadRecordsResponseJson, remoteReadRecordsResponseConstructedToJson);
    }

    private Schema getGetTableRequestsSchema()
    {
        return SchemaBuilder.newBuilder()
            .addField(yearCol, new ArrowType.Int(32, true))
            .addField(monthCol, new ArrowType.Int(32, true))
            .addField(dayCol, new ArrowType.Int(32, true))
            .addField("col3", new ArrowType.Utf8())
            .build();
    }


    private String readFileAsJsonString(String locationHint, String resource) throws IOException
    {
        return utils.readAllAsString(utils.getResourceOrFail(locationHint, resource)).trim();
    }

    private void assertTypeHeaderParsesCorrectly(String inputJson, String type) throws InvalidProtocolBufferException
    {
        TypeHeader typeHeader = (TypeHeader) ProtobufSerDe.buildFromJson(inputJson, TypeHeader.newBuilder());
        assertEquals(type, typeHeader.getType());
    }

    private void assertJsonEquivalentIgnoringWhitespace(String jsonFromFile, String constructedJson)
    {
        assertEquals(jsonFromFile.replaceAll(" ", "").replaceAll("\n", ""), constructedJson.replaceAll(" ", "").replaceAll("\n", ""));
        // assertEquals(jsonFromFile.replaceAll(" : ", ": "), constructedJson);
    }

}

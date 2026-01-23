/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.deltashare.client.DeltaShareClient;
import com.amazonaws.athena.connectors.deltashare.constants.DeltaShareConstants;
import com.amazonaws.athena.connectors.deltashare.model.DeltaShareTable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TestBase
{
    protected static final String TEST_CATALOG_NAME = "deltashare";
    protected static final String TEST_QUERY_ID = "test-query-id";
    protected static final String TEST_SHARE_NAME = "test-share";
    protected static final String TEST_SCHEMA_NAME = "test-schema";
    protected static final String TEST_TABLE_NAME = "test-table";
    protected static final String TEST_ENDPOINT = "https://test-endpoint.com";
    protected static final String TEST_TOKEN = "test-token";
    
    protected static final TableName TEST_TABLE = new TableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME);
    protected static final FederatedIdentity TEST_IDENTITY = new FederatedIdentity(
        "arn:aws:sts::123456789012:assumed-role/test-role/test-user",
        "123456789012",
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyMap()
    );

    protected BlockAllocator allocator;
    protected ObjectMapper objectMapper;
    protected Map<String, String> configOptions;
    
    @Mock
    protected DeltaShareClient mockDeltaShareClient;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        objectMapper = new ObjectMapper();
        configOptions = ImmutableMap.of(
            DeltaShareConstants.ENDPOINT_PROPERTY, TEST_ENDPOINT,
            DeltaShareConstants.TOKEN_PROPERTY, TEST_TOKEN,
            DeltaShareConstants.SHARE_NAME_PROPERTY, TEST_SHARE_NAME
        );
        mockDeltaShareClient = mock(DeltaShareClient.class);
    }

    @After
    public void tearDown()
    {
        if (allocator != null) {
            allocator.close();
        }
    }

    protected QueryPlan createQueryPlan(String plan)
    {
        return new QueryPlan("", plan);
    }

    protected Schema createTestSchema()
    {
        return SchemaBuilder.newBuilder()
            .addStringField("col1")
            .addIntField("col2")
            .addBigIntField("col3")
            .addFloat8Field("col4")
            .addBitField("col5")
            .addDateDayField("col6")
            .addDateMilliField("col7")
            .addMetadata("source", "deltashare")
            .addMetadata("share", TEST_SHARE_NAME)
            .addMetadata("schema", TEST_SCHEMA_NAME)
            .addMetadata("table", TEST_TABLE_NAME)
            .build();
    }

    protected Schema createPartitionedTestSchema()
    {
        return SchemaBuilder.newBuilder()
            .addStringField("partition_col")
            .addStringField("col1")
            .addIntField("col2")
            .addMetadata("source", "deltashare")
            .addMetadata("share", TEST_SHARE_NAME)
            .addMetadata("schema", TEST_SCHEMA_NAME)
            .addMetadata("table", TEST_TABLE_NAME)
            .addMetadata("partitionCols", "partition_col")
            .build();
    }

    protected List<String> createMockSchemaList()
    {
        return Arrays.asList("schema1", "schema2", TEST_SCHEMA_NAME);
    }

    protected List<DeltaShareTable> createMockTableList() throws Exception
    {
        JsonNode mockSchema = createMockTableMetadata();
        return Arrays.asList(
            new DeltaShareTable("table1", mockSchema),
            new DeltaShareTable("table2", mockSchema),
            new DeltaShareTable(TEST_TABLE_NAME, mockSchema)
        );
    }

    protected JsonNode createMockTableMetadata() throws Exception
    {
        String metadataJson = "{\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"col1\", \"type\": \"string\", \"nullable\": true},\n" +
            "    {\"name\": \"col2\", \"type\": \"integer\", \"nullable\": false},\n" +
            "    {\"name\": \"col3\", \"type\": \"long\", \"nullable\": true},\n" +
            "    {\"name\": \"col4\", \"type\": \"double\", \"nullable\": true},\n" +
            "    {\"name\": \"col5\", \"type\": \"boolean\", \"nullable\": true},\n" +
            "    {\"name\": \"col6\", \"type\": \"date\", \"nullable\": true},\n" +
            "    {\"name\": \"col7\", \"type\": \"timestamp\", \"nullable\": true}\n" +
            "  ],\n" +
            "  \"partitionColumns\": []\n" +
            "}";
        return objectMapper.readTree(metadataJson);
    }

    protected JsonNode createMockPartitionedTableMetadata() throws Exception
    {
        String metadataJson = "{\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"partition_col\", \"type\": \"string\", \"nullable\": true},\n" +
            "    {\"name\": \"col1\", \"type\": \"string\", \"nullable\": true},\n" +
            "    {\"name\": \"col2\", \"type\": \"integer\", \"nullable\": false}\n" +
            "  ],\n" +
            "  \"partitionColumns\": [\"partition_col\"]\n" +
            "}";
        return objectMapper.readTree(metadataJson);
    }

    protected JsonNode createMockQueryResponse() throws Exception
    {
        String queryResponseJson = "[\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://presigned-url-1.com/file1.parquet\",\n" +
            "      \"size\": 1048576,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://presigned-url-2.com/file2.parquet\",\n" +
            "      \"size\": 2097152,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  }\n" +
            "]";
        return objectMapper.readTree(queryResponseJson);
    }

    protected JsonNode createMockPartitionedQueryResponse() throws Exception
    {
        String queryResponseJson = "[\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://presigned-url-1.com/file1.parquet\",\n" +
            "      \"size\": 1048576,\n" +
            "      \"partitionValues\": {\"partition_col\": \"value1\"}\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://presigned-url-2.com/file2.parquet\",\n" +
            "      \"size\": 2097152,\n" +
            "      \"partitionValues\": {\"partition_col\": \"value2\"}\n" +
            "    }\n" +
            "  }\n" +
            "]";
        return objectMapper.readTree(queryResponseJson);
    }

    protected JsonNode createMockSingleLargeFileQueryResponse() throws Exception
    {
        String queryResponseJson = "[\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://presigned-url-large.com/large-file.parquet\",\n" +
            "      \"size\": 134217728,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  }\n" +
            "]";
        return objectMapper.readTree(queryResponseJson);
    }

    protected JsonNode createMockEmptyQueryResponse() throws Exception
    {
        return objectMapper.readTree("[]");
    }

    protected JsonNode createMockMultiFileQueryResponse() throws Exception
    {
        String queryResponseJson = "[\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://test-url1.com/file1.parquet\",\n" +
            "      \"size\": 1048576,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://test-url2.com/file2.parquet\",\n" +
            "      \"size\": 2097152,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://test-url3.com/file3.parquet\",\n" +
            "      \"size\": 3145728,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  }\n" +
            "]";
        return objectMapper.readTree(queryResponseJson);
    }

    protected JsonNode createMockLargeFileQueryResponse() throws Exception
    {
        StringBuilder jsonBuilder = new StringBuilder("[\n");
        for (int i = 0; i < 15; i++) {
            if (i > 0) {
                jsonBuilder.append(",\n");
            }
            jsonBuilder.append("  {\n")
                      .append("    \"file\": {\n")
                      .append("      \"url\": \"https://test-url").append(i).append(".com/file").append(i).append(".parquet\",\n")
                      .append("      \"size\": ").append(1048576 + i * 100000).append(",\n")
                      .append("      \"partitionValues\": {}\n")
                      .append("    }\n")
                      .append("  }");
        }
        jsonBuilder.append("\n]");
        return objectMapper.readTree(jsonBuilder.toString());
    }

    protected JsonNode createMockMalformedFileQueryResponse() throws Exception
    {
        String queryResponseJson = "[\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"size\": 1048576,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"\",\n" +
            "      \"size\": 2097152,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"file\": {\n" +
            "      \"url\": \"https://valid-url.com/file.parquet\",\n" +
            "      \"size\": 3145728,\n" +
            "      \"partitionValues\": {}\n" +
            "    }\n" +
            "  }\n" +
            "]";
        return objectMapper.readTree(queryResponseJson);
    }

    protected JsonNode createMockRealDeltaShareResponse() throws Exception
    {
        String queryResponseJson = "[\n" +
            "  {\"protocol\":{\"minReaderVersion\":1}},\n" +
            "  {\"metaData\":{\"id\":\"ad5665ff-9dc5-465a-6ee6-e4145fcb409e\",\"format\":{\"provider\":\"parquet\"},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"description\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"amount\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"created_date\\\",\\\"type\\\":\\\"timestamp\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"size\":1799251443,\"numFiles\":14}},\n" +
            "  {\"file\":{\"url\":\"https://testdeltasharetablebucket-942204942515.s3-fips.us-east-1.amazonaws.com/call_center_delta_new/part-00000-e896cd28-7514-4f10-b1fc-14ba88b29d23.c000.snappy.parquet\",\"expirationTimestamp\":1758829167000,\"id\":\"3531e91a40451b24e715f776af7e240f\",\"partitionValues\":{},\"size\":133451873}},\n" +
            "  {\"file\":{\"url\":\"https://testdeltasharetablebucket-942204942515.s3-fips.us-east-1.amazonaws.com/call_center_delta_new/part-00001-b2f224b2-d622-4611-87c3-a42c0763a057.c000.snappy.parquet\",\"expirationTimestamp\":1758829167000,\"id\":\"16c50bb378fc43ca81024c8d02fe2d89\",\"partitionValues\":{},\"size\":133509375}},\n" +
            "  {\"file\":{\"url\":\"https://testdeltasharetablebucket-942204942515.s3-fips.us-east-1.amazonaws.com/call_center_delta_new/part-00002-4a0f5a34-6b63-4f30-87e3-506050bed9f9.c000.snappy.parquet\",\"expirationTimestamp\":1758829167000,\"id\":\"577cfce3fb7be160382a090a1bdad22d\",\"partitionValues\":{},\"size\":133131295}}\n" +
            "]";
        return objectMapper.readTree(queryResponseJson);
    }
}

/*-
 * #%L
 * athena-example
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
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
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
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import io.findify.s3mock.S3Mock;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.*;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_FILE_PROPERTY;
import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.SPLIT_PARTITION_VALUES_PROPERTY;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class DeltalakeMetadataHandlerTest extends TestBase
{
    private DeltalakeMetadataHandler handler;

    private static final Logger logger = LoggerFactory.getLogger(DeltalakeMetadataHandlerTest.class);

    private BlockAllocatorImpl allocator;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() {
        logger.info("{}: enter ", testName.getMethodName());
        allocator = new BlockAllocatorImpl();
        String dataBucket = "test-bucket-1";

        this.handler = new DeltalakeMetadataHandler(
                amazonS3,
                new LocalKeyFactory(),
                mock(AWSSecretsManager.class),
                mock(AmazonAthena.class),
                "spill-bucket",
                "spill-prefix",
                dataBucket);
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNames()
    {
        // Given
        ListSchemasRequest req = new ListSchemasRequest(fakeIdentity(), "queryId", "default");
        // When
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        // Then
        assertFalse(res.getSchemas().isEmpty());
        assertArrayEquals(Arrays.asList("test-database-2", "test-database-1").toArray(), res.getSchemas().toArray());
    }

    @Test
    public void doListTablesUnlimited()
    {
        // Given
        String schemaName = "test-database-1";
        ListTablesRequest request = new ListTablesRequest(fakeIdentity(), "queryId", "default",
                schemaName, null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse expectedResponse = new ListTablesResponse("default",
                new ImmutableList.Builder<TableName>()
                        .add(new TableName(schemaName, "test-table-1"))
                        .add(new TableName(schemaName, "test-table-2"))
                        .add(new TableName(schemaName, "test-table-3"))
                        .build(), null);

        // When
        ListTablesResponse response = handler.doListTables(allocator, request);

        // Then
        assertEquals(expectedResponse, response);
    }

    @Test
    public void doListTablesWithPageSize()
    {
        // First request
        String schemaName = "test-database-1";
        int pageSize = 2;
        ListTablesRequest request1 = new ListTablesRequest(fakeIdentity(), "queryId", "default",
                schemaName, null, pageSize);
        ListTablesResponse expectedResponse1 = new ListTablesResponse("default",
                new ImmutableList.Builder<TableName>()
                        .add(new TableName(schemaName, "test-table-1"))
                        .add(new TableName(schemaName, "test-table-2"))
                        .build(), "test-database-1/test-table-2_$folder$");
        ListTablesResponse response1 = handler.doListTables(allocator, request1);
        assertEquals(expectedResponse1, response1);
    }

    @Test
    public void doGetTable() throws IOException {
        // Given
        String schemaName = "test-database-1";
        String tableName = "test-table-1";
        GetTableRequest req = new GetTableRequest(fakeIdentity(), "queryId", "default",
                new TableName(schemaName, tableName));

        // When
        GetTableResponse res = handler.doGetTable(allocator, req);

        // Then
        assertTrue(res.getSchema().getFields().size() > 0);
        assertTrue(res.getPartitionColumns().size() == 0);
    }

    @Test
    public void getPartitions()
            throws Exception
    {
        // Given
        String schemaName = "test-database-2";
        String tableName = "partitioned-table";
        TableName table = new TableName(schemaName, tableName);

        Schema tableSchema = SchemaBuilder.newBuilder()
                .addBitField("is_valid")
                .addStringField("region")
                .addDateDayField("event_date")
                .addIntField("amount")
                .build();

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("is_valid");
        partitionCols.add("region");
        partitionCols.add("event_date");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        GetTableLayoutRequest req = new GetTableLayoutRequest(fakeIdentity(), "queryId", "default",
            table,
            new Constraints(constraintsMap),
            tableSchema,
            partitionCols);

        // When
        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        // Then
        Block partitions = res.getPartitions();
        assertEquals(2, partitions.getRowCount());
        assertEquals("[is_valid : true], [event_date : 18617], [region : asia]", BlockUtils.rowToString(partitions, 0));
        assertEquals("[is_valid : false], [event_date : 18669], [region : europe]", BlockUtils.rowToString(partitions, 1));
    }

    @Test
    public void doGetSplits() throws IOException {
        // Given
        String schemaName = "test-database-2";
        String tableName = "partitioned-table";
        TableName table = new TableName(schemaName, tableName);

        String isValidCol = "is_valid";
        String regionCol = "region";
        String eventDateCol = "event_date";

        Schema tableSchema = SchemaBuilder.newBuilder()
                .addBitField(isValidCol)
                .addStringField(regionCol)
                .addDateDayField(eventDateCol)
                .addIntField("amount")
                .build();

        List<String> partitionCols = Arrays.asList(isValidCol, regionCol, eventDateCol);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Block partitions = allocator.createBlock(tableSchema);

        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(isValidCol), i, true);
            BlockUtils.setValue(partitions.getFieldVector(regionCol), i, new Text("a"));
            BlockUtils.setValue(partitions.getFieldVector(eventDateCol), i, LocalDate.of(2021, 3, 24));
        }
        partitions.setRowCount(num_partitions);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(fakeIdentity(), "queryId", "catalog_name",
                table,
                partitions,
                partitionCols,
                new Constraints(constraintsMap),
                continuationToken);
        int numContinuations = 0;
        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

            MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
            assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

            GetSplitsResponse response = (GetSplitsResponse) rawResponse;
            continuationToken = response.getContinuationToken();

            for (Split nextSplit : response.getSplits()) {
                assertNotNull(nextSplit.getProperty(SPLIT_PARTITION_VALUES_PROPERTY));
                assertNotNull(nextSplit.getProperty(SPLIT_FILE_PROPERTY));
            }

            assertTrue(!response.getSplits().isEmpty());

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertTrue(numContinuations == 0);
    }

    private static FederatedIdentity fakeIdentity()
    {
        return new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    }
}

/*-
 * #%L
 * athena-deltalake
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
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Supplier;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class DeltalakeMetadataHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeMetadataHandlerTest.class);

    private DeltalakeMetadataHandler handler;

    private BlockAllocatorImpl allocator;

    private final EncryptionKey encryptionKey = new EncryptionKey(
        new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
        new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});

    private final Supplier<SchemaBuilder> partitionsSchemaBuilder = () -> SchemaBuilder.newBuilder()
        .addBitField("is_valid")
        .addStringField("region")
        .addDateDayField("event_date");

    private final Schema enhancedPartitionsSchema = partitionsSchemaBuilder.get()
        .addStringField(ENHANCED_FILE_PATH_COLUMN)
        .addStringField(ENHANCED_PARTITION_VALUES_COLUMN)
        .build();

    private final Schema partitionedTableSchema = partitionsSchemaBuilder.get()
        .addIntField("amount")
        .build();

    private final Set<String> partitionCols = ImmutableSet.of("is_valid", "region", "event_date");

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() {
        logger.info("{}: enter ", testName.getMethodName());
        allocator = new BlockAllocatorImpl();
        String dataBucket = "test-bucket-1";

        this.handler = new DeltalakeMetadataHandler(
            amazonS3,
            () -> encryptionKey,
            mock(AWSSecretsManager.class),
            mock(AmazonAthena.class),
            spillBucket,
            spillPrefix,
            dataBucket);
    }

    @After
    public void tearDown()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNames()
    {
        // Given
        ListSchemasRequest req = new ListSchemasRequest(fakeIdentity(), "queryId", "default");
        ListSchemasResponse expectedResponse = new ListSchemasResponse("default",
            new ImmutableList.Builder<String>()
                .add("test-database-2")
                .add("test-database-1")
                .build());
        // When
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        // Then
        assertEquals(expectedResponse, res);
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
        GetTableResponse expectedResponse = new GetTableResponse(
            "default",
            new TableName("test-database-1", "test-table-1"),
            new SchemaBuilder()
                .addDateMilliField("timestamp")
                .addDateDayField("date")
                .build(),
            Collections.emptySet());

        // When
        GetTableResponse res = handler.doGetTable(allocator, req);

        // Then
        assertEquals(expectedResponse, res);
    }

    @Test
    public void getPartitions()
            throws Exception
    {
        // Given
        String catalogName = "default";
        String schemaName = "test-database-2";
        String tableName = "partitioned-table";
        TableName table = new TableName(schemaName, tableName);

        Block expectedPartitions = allocator.createBlock(enhancedPartitionsSchema);

        BlockUtils.setValue(expectedPartitions.getFieldVector("is_valid"), 0,  false);
        BlockUtils.setValue(expectedPartitions.getFieldVector("region"), 0,  "europe");
        BlockUtils.setValue(expectedPartitions.getFieldVector("event_date"), 0,  LocalDate.of(2021, 2, 11));
        BlockUtils.setValue(expectedPartitions.getFieldVector(ENHANCED_FILE_PATH_COLUMN), 0, "is_valid=false/region=europe/event_date=2021-02-11/part-00000-58828e3c-041e-47b4-80dd-196ae1b1d1a6-c000.snappy.parquet");
        BlockUtils.setValue(expectedPartitions.getFieldVector(ENHANCED_PARTITION_VALUES_COLUMN), 0, "{\"is_valid\":\"false\",\"event_date\":\"2021-02-11\",\"region\":\"europe\"}");

        BlockUtils.setValue(expectedPartitions.getFieldVector("is_valid"), 1,  true);
        BlockUtils.setValue(expectedPartitions.getFieldVector("region"), 1,  "asia");
        BlockUtils.setValue(expectedPartitions.getFieldVector("event_date"), 1,  LocalDate.of(2021, 12, 21));
        BlockUtils.setValue(expectedPartitions.getFieldVector(ENHANCED_FILE_PATH_COLUMN), 1, "is_valid=true/region=asia/event_date=2021-12-21/part-00000-58828e3c-041e-47b4-80dd-196ae1b1d1a6-c000.snappy.parquet");
        BlockUtils.setValue(expectedPartitions.getFieldVector(ENHANCED_PARTITION_VALUES_COLUMN), 1, "{\"is_valid\":\"true\",\"event_date\":\"2021-12-21\",\"region\":\"asia\"}");

        expectedPartitions.setRowCount(2);

        ArrowType dateType = partitionedTableSchema.findField("event_date").getType();
        int lowerDateBound = Math.toIntExact(LocalDate.parse("2021-01-01").toEpochDay());
        Map<String, ValueSet> constraintsMap = ImmutableMap.<String, ValueSet>builder()
            .put("event_date",
                SortedRangeSet
                    .newBuilder(dateType, false)
                    .add(Range.greaterThan(allocator, dateType, lowerDateBound))
                    .build()
            ).build();

        GetTableLayoutRequest req = new GetTableLayoutRequest(fakeIdentity(), "queryId", "default",
            table,
            new Constraints(constraintsMap),
            partitionedTableSchema,
            partitionCols);

        GetTableLayoutResponse expectedResponse = new GetTableLayoutResponse(catalogName, table, expectedPartitions);

        // When
        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        // Then
        Block partitions = res.getPartitions();
        assertEquals(2, partitions.getRowCount());
        assertEquals(expectedResponse, res);
    }

    @Test
    public void doGetSplits() {
        // Given
        UUID splitUuid = UUID.fromString("123e4567-e89b-12d3-a456-426652340000");
        MockedStatic<UUID> utilities = Mockito.mockStatic(UUID.class);
        utilities.when(UUID::randomUUID).thenReturn(splitUuid);
        String catalogName = "catalog_name";
        String schemaName = "test-database-2";
        String tableName = "partitioned-table";
        TableName table = new TableName(schemaName, tableName);

        ArrowType dateType = partitionedTableSchema.findField("event_date").getType();
        int lowerDateBound = Math.toIntExact(LocalDate.parse("2021-01-01").toEpochDay());
        Map<String, ValueSet> constraintsMap = ImmutableMap.<String, ValueSet>builder()
            .put("event_date",
                SortedRangeSet
                    .newBuilder(dateType, false)
                    .add(Range.greaterThan(allocator, dateType, lowerDateBound))
                    .build()
            ).build();

        Block partitions = allocator.createBlock(enhancedPartitionsSchema);

        int num_partitions = MAX_SPLITS_PER_REQUEST + MAX_SPLITS_PER_REQUEST / 2;
        for (int i = 0; i < num_partitions; i++) {
            String partitionValues = "{\"is_valid\":\"false\",\"event_date\":\"2021-02-11\",\"region\":\"europe\"}";
            String filePath = String.format("is_valid=false/region=europe/event_date=2021-02-11/part-00000-58828e3c-041e-47b4-80dd-196ae1b1d1a6-%d.snappy.parquet", i);
            BlockUtils.setValue(partitions.getFieldVector("is_valid"), i,  false);
            BlockUtils.setValue(partitions.getFieldVector("region"), i,  "europe");
            BlockUtils.setValue(partitions.getFieldVector("event_date"), i,  LocalDate.of(2021, 2, 11));
            BlockUtils.setValue(partitions.getFieldVector(ENHANCED_FILE_PATH_COLUMN), i, filePath);
            BlockUtils.setValue(partitions.getFieldVector(ENHANCED_PARTITION_VALUES_COLUMN), i, partitionValues);
        }
        partitions.setRowCount(num_partitions);

        GetSplitsRequest originalReq = new GetSplitsRequest(fakeIdentity(), "queryId", "catalog_name",
            table,
            partitions,
            new ArrayList<>(partitionCols),
            new Constraints(constraintsMap),
            null);

        // First batch of splits
        GetSplitsRequest request1 = new GetSplitsRequest(originalReq, null);
        Set<Split> expectedSplits1 = new HashSet<>();
        int expectedSplitSize1 = MAX_SPLITS_PER_REQUEST;
        for (int i = 0; i < expectedSplitSize1 ; i++) {
            String filePath = String.format("is_valid=false/region=europe/event_date=2021-02-11/part-00000-58828e3c-041e-47b4-80dd-196ae1b1d1a6-%d.snappy.parquet", i);
            String partitionValues = "{\"is_valid\":\"false\",\"event_date\":\"2021-02-11\",\"region\":\"europe\"}";
            expectedSplits1.add(
                Split.newBuilder(makeSpillLocation(request1.getQueryId(), splitUuid.toString()), encryptionKey)
                    .add(SPLIT_FILE_PROPERTY, filePath)
                    .add(SPLIT_PARTITION_VALUES_PROPERTY, partitionValues)
                    .build()
            );
        }

        String expectedContinuationToken1 = "999";
        GetSplitsResponse expectedResponse1 = new GetSplitsResponse(
                catalogName, expectedSplits1, expectedContinuationToken1
        );
        GetSplitsResponse response1 = handler.doGetSplits(allocator, request1);

        assertEquals(expectedResponse1, response1);

        // End of splits
        GetSplitsRequest request2 = new GetSplitsRequest(originalReq, expectedContinuationToken1);
        Set<Split> expectedSplits2 = new HashSet<>();
        int expectedSplitSize2 = MAX_SPLITS_PER_REQUEST / 2;
        for (int i = expectedSplitSize1; i < expectedSplitSize1 + expectedSplitSize2 ; i++) {
            String filePath = String.format("is_valid=false/region=europe/event_date=2021-02-11/part-00000-58828e3c-041e-47b4-80dd-196ae1b1d1a6-%d.snappy.parquet", i);
            String partitionValues = "{\"is_valid\":\"false\",\"event_date\":\"2021-02-11\",\"region\":\"europe\"}";
            expectedSplits2.add(
                Split.newBuilder(makeSpillLocation(request1.getQueryId(), splitUuid.toString()), encryptionKey)
                    .add(SPLIT_FILE_PROPERTY, filePath)
                    .add(SPLIT_PARTITION_VALUES_PROPERTY, partitionValues)
                    .build()
            );
        }

        GetSplitsResponse expectedResponse2 = new GetSplitsResponse(
            catalogName, expectedSplits2, null
        );
        GetSplitsResponse response2 = handler.doGetSplits(allocator, request2);

        assertEquals(expectedResponse2, response2);
    }
}

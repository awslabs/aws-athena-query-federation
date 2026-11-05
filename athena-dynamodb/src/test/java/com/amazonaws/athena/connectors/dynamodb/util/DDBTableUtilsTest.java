/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.util;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Covers scan-segment sizing, in particular the on-demand (PAY_PER_REQUEST) path.
 *
 * <p>DescribeTable populates {@code provisionedThroughput} for both billing modes — an on-demand
 * table reports it non-null with 0 RCU — so detecting on-demand by a null check silently yields a
 * capacity of 0 and therefore a single, serial scan segment for any table under
 * {@code MAX_BYTES_PER_SEGMENT}. These tests pin the detection and the segment counts either side
 * of it.
 */
@RunWith(MockitoJUnitRunner.class)
public class DDBTableUtilsTest
{
    private static final ThrottlingInvoker.ExceptionFilter NO_OP_FILTER = ex -> false;
    private static final String TABLE_NAME = "test_table";

    private static final long MB = 1024L * 1024L;
    private static final long GB = 1024L * MB;

    private ThrottlingInvoker invoker;

    @Mock
    private DynamoDbClient ddbClient;

    @Before
    public void setUp()
    {
        invoker = ThrottlingInvoker.newDefaultBuilder(NO_OP_FILTER, Collections.emptyMap()).build();
    }

    // =========================================================================
    // On-demand detection (regression guard)
    // =========================================================================

    /**
     * An on-demand table reports a non-null ProvisionedThroughput with 0 RCU. The resolved capacity
     * must not be 0 — that is what collapsed small on-demand tables to one segment.
     */
    @Test
    public void onDemandTableDoesNotResolveToZeroCapacity() throws TimeoutException
    {
        DynamoDBTable table = getTableWithThroughput(0L, 512 * MB);

        assertTrue("on-demand table must not resolve to 0 read capacity",
                table.getProvisionedReadCapacity() > 0);
    }

    /** A provisioned table must keep using its declared RCU, unchanged by the on-demand path. */
    @Test
    public void provisionedTableUsesDeclaredCapacity() throws TimeoutException
    {
        DynamoDBTable table = getTableWithThroughput(250L, 512 * MB);

        assertEquals(250L, table.getProvisionedReadCapacity());
    }

    /** A genuinely absent ProvisionedThroughput is treated as on-demand, not as 0 capacity. */
    @Test
    public void nullThroughputIsTreatedAsOnDemand() throws TimeoutException
    {
        DynamoDBTable table = getTableWithThroughput(null, 512 * MB);

        assertTrue("absent throughput must not resolve to 0 read capacity",
                table.getProvisionedReadCapacity() > 0);
    }

    // =========================================================================
    // Segment counts for on-demand tables
    // =========================================================================

    /**
     * A small on-demand table must stay at a single segment. Substituting the account-default
     * on-demand ceiling (40k RCU) without bounding it by data volume would produce ~400 segments
     * here, each scanning a near-empty slice and paying full per-split fixed cost.
     */
    @Test
    public void smallOnDemandTableStaysSingleSegment() throws TimeoutException
    {
        DynamoDBTable table = getTableWithThroughput(0L, 10 * MB);

        assertEquals(1, segmentsFor(table));
    }

    /**
     * A sub-1-GiB on-demand table must now parallelise, where previously it could not. 900 MB at
     * 128 MiB per segment is 7, below the 32-segment cap.
     */
    @Test
    public void mediumOnDemandTableParallelises() throws TimeoutException
    {
        DynamoDBTable table = getTableWithThroughput(0L, 900 * MB);

        assertEquals(7, segmentsFor(table));
    }

    /**
     * 2 GiB is a benchmarked size: parallel Scan throughput peaked at 16 segments on a real
     * on-demand table, and 32 segments measured ~1.6x slower. 2048 MiB / 128 MiB = 16, under the
     * cap, so the size bound governs here.
     */
    @Test
    public void twoGibOnDemandTableMatchesMeasuredOptimum() throws TimeoutException
    {
        DynamoDBTable table = getTableWithThroughput(0L, 2 * GB);

        assertEquals(16, segmentsFor(table));
    }

    /**
     * 10 GiB is the other benchmarked size: throughput peaked at 32 segments, and 64 measured
     * ~1.3x slower. The size bound alone would give 80 (10240 MiB / 128 MiB), so this is the case
     * MAX_ON_DEMAND_SEGMENTS exists to catch -- it is the regression guard for the cap.
     */
    @Test
    public void tenGibOnDemandTableIsCappedAtMeasuredOptimum() throws TimeoutException
    {
        DynamoDBTable table = getTableWithThroughput(0L, 10 * GB);

        assertEquals(32, segmentsFor(table));
    }

    /**
     * Past roughly 32 GiB the size term in {@link DDBTableUtils#getNumSegments} (one segment per
     * GiB) exceeds the capped throughput term, so it takes over and the cap stops binding. Pins
     * that handover so a future cap change does not silently alter very large tables.
     */
    @Test
    public void veryLargeOnDemandTableFallsBackToSizeTerm() throws TimeoutException
    {
        DynamoDBTable table = getTableWithThroughput(0L, 100 * GB);

        assertEquals(100, segmentsFor(table));
    }

    // =========================================================================
    // getNumSegments bounds
    // =========================================================================

    @Test
    public void segmentCountIsNeverBelowOne()
    {
        assertEquals(1, DDBTableUtils.getNumSegments(0L, 0L));
    }

    @Test
    public void sizeDominatesForVeryLargeTables()
    {
        // 2 TiB at 1 GiB per segment == 2048 segments for size, which exceeds the 400 the
        // on-demand pseudo-capacity ceiling would give.
        assertEquals(2048, DDBTableUtils.getNumSegments(40_000L, 2048 * GB));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private int segmentsFor(DynamoDBTable table)
    {
        return DDBTableUtils.getNumSegments(
                table.getProvisionedReadCapacity(), table.getApproxTableSizeInBytes());
    }

    /**
     * @param readCapacityUnits declared RCU, or {@code null} to omit ProvisionedThroughput entirely
     */
    private DynamoDBTable getTableWithThroughput(Long readCapacityUnits, long tableSizeBytes)
            throws TimeoutException
    {
        TableDescription.Builder description = TableDescription.builder()
                .tableName(TABLE_NAME)
                .keySchema(KeySchemaElement.builder()
                        .attributeName("hashKey")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(Collections.emptyList())
                .tableSizeBytes(tableSizeBytes)
                .itemCount(1L);

        if (readCapacityUnits != null) {
            description.provisionedThroughput(ProvisionedThroughputDescription.builder()
                    .readCapacityUnits(readCapacityUnits)
                    .writeCapacityUnits(0L)
                    .build());
        }

        when(ddbClient.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(DescribeTableResponse.builder().table(description.build()).build());

        return DDBTableUtils.getTable(TABLE_NAME, invoker, ddbClient, null);
    }
}

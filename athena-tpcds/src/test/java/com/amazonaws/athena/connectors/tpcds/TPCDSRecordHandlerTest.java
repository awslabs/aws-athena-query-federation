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
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.tpcds.qpt.TPCDSQueryPassthrough;
import com.google.common.io.ByteStreams;
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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TPCDSRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSRecordHandlerTest.class);
    private static final String TEST_CATALOG = "catalog";
    private static final String TEST_SCHEMA_TPCDS1 = "tpcds1";

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private List<ByteHolder> mockS3Storage;
    private TPCDSRecordHandler handler;
    private S3BlockSpillReader spillReader;
    private BlockAllocator allocator;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private Table table;
    private Schema schemaForRead;

    @Mock
    private S3Client mockS3;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
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

        when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return PutObjectResponse.builder().build();
                });

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                });
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void doReadRecords_WhenResultFitsInMemory_ReturnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecordsNoSpill: enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("c_customer_id", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                .add("AAAAAAAABAAAAAAA")
                .add("AAAAAAAACAAAAAAA")
                .add("AAAAAAAADAAAAAAA").build());

        ReadRecordsRequest request = newReadRecordsRequest(schemaForRead, table.getName(),
                newSplit("0", "1000", "1"),
                createConstraints(constraintsMap, Collections.emptyMap()),
                100_000_000_000L,
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() == 3);
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void doReadRecords_WhenResultExceedsSpillThreshold_ReturnsRemoteReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("c_current_cdemo_sk", SortedRangeSet.of(
                Range.range(allocator, Types.MinorType.BIGINT.getType(), 100L, true, 100_000_000L, true)));

        ReadRecordsRequest request = newReadRecordsRequest(schemaForRead, table.getName(),
                newSplit("0", "10000", "1"),
                createConstraints(constraintsMap, Collections.emptyMap()),
                1_500_000L,
                0);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue(response.getNumberBlocks() > 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                    logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                    // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

                    logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }

        logger.info("doReadRecordsSpill: exit");
    }

    @Test
    public void doReadRecords_WhenTableHasTimeTypeColumn_ReturnsRecords()
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

        ReadRecordsRequest request = newReadRecordsRequest(schemaBuilder.build(), table.getName(),
                newSplit("0", "1000", "1"),
                createConstraints(Collections.emptyMap(), Collections.emptyMap()),
                100_000_000_000L,
                100_000_000_000L);
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

        logger.info("doReadRecordForTPCDSTIMETypeColumn: {}", BlockUtils.rowToString(response.getRecords(), 0));
        assertEquals(1, response.getRecords().getRowCount()); // TPCDS for `dbgen_version` always generates 1 record.

        logger.info("doReadRecordForTPCDSTIMETypeColumn: exit");
    }

    @Test(expected = NumberFormatException.class)
    public void doReadRecords_WhenSplitMissingScaleFactorProperty_ThrowsNumberFormatException()
            throws Exception
    {
        ReadRecordsRequest request = newReadRecordsRequest(schemaForRead, table.getName(),
                newSplit("0", "1000", null),
                createConstraints(Collections.emptyMap(), Collections.emptyMap()),
                100_000_000_000L,
                100_000_000_000L);
        handler.doReadRecords(allocator, request);
    }

    @Test(expected = NumberFormatException.class)
    public void doReadRecords_WhenSplitHasNonNumericScaleFactor_ThrowsNumberFormatException()
            throws Exception
    {
        ReadRecordsRequest request = newReadRecordsRequest(schemaForRead, table.getName(),
                newSplit("0", "1000", "not_a_number"),
                createConstraints(Collections.emptyMap(), Collections.emptyMap()),
                100_000_000_000L,
                100_000_000_000L);
        handler.doReadRecords(allocator, request);
    }

    @Test(expected = RuntimeException.class)
    public void doReadRecords_WhenUnknownTableInRequest_ThrowsRuntimeException()
            throws Exception
    {
        ReadRecordsRequest request = newReadRecordsRequest(schemaForRead, "nonexistent_table",
                newSplit("0", "1000", "1"),
                createConstraints(Collections.emptyMap(), Collections.emptyMap()),
                100_000_000_000L,
                100_000_000_000L);
        handler.doReadRecords(allocator, request);
    }

    @Test
    public void doReadRecords_WhenQueryPassthroughWithValidTable_ReturnsReadRecordsResponse()
            throws Exception
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_CATALOG, TEST_CATALOG);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_SCHEMA, TEST_SCHEMA_TPCDS1);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_TABLE, "customer");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("c_customer_id", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                .add("AAAAAAAABAAAAAAA")
                .add("AAAAAAAACAAAAAAA")
                .add("AAAAAAAADAAAAAAA").build());

        ReadRecordsRequest request = newReadRecordsRequest(schemaForRead, "ignored_table_name",
                newSplit("0", "1000", "1"),
                createConstraints(constraintsMap, qptArguments),
                100_000_000_000L,
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertEquals(3, response.getRecords().getRowCount());
    }

    @Test(expected = RuntimeException.class)
    public void doReadRecords_WhenQueryPassthroughWithUnknownTable_ThrowsRuntimeException()
            throws Exception
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_CATALOG, TEST_CATALOG);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_SCHEMA, TEST_SCHEMA_TPCDS1);
        qptArguments.put(TPCDSQueryPassthrough.TPCDS_TABLE, "nonexistent_table");

        ReadRecordsRequest request = newReadRecordsRequest(schemaForRead, "customer",
                newSplit("0", "1000", "1"),
                createConstraints(Collections.emptyMap(), qptArguments),
                100_000_000_000L,
                100_000_000_000L);

        handler.doReadRecords(allocator, request);
    }

    @Test
    public void doReadRecords_WhenProjectingSubsetOfCustomerColumns_ReturnsReadRecordsResponse()
            throws Exception
    {
        SchemaBuilder subsetBuilder = SchemaBuilder.newBuilder();
        for (Column nextCol : table.getColumns()) {
            if ("c_customer_sk".equals(nextCol.getName()) || "c_first_name".equals(nextCol.getName())) {
                subsetBuilder.addField(TPCDSUtils.convertColumn(nextCol));
            }
        }
        Schema subsetSchema = subsetBuilder.build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("c_customer_id", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                .add("AAAAAAAABAAAAAAA")
                .add("AAAAAAAACAAAAAAA")
                .add("AAAAAAAADAAAAAAA").build());

        ReadRecordsRequest request = newReadRecordsRequest(subsetSchema, table.getName(),
                newSplit("0", "1000", "1"),
                createConstraints(constraintsMap, Collections.emptyMap()),
                100_000_000_000L,
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertEquals(2, response.getSchema().getFields().size());
        assertTrue("Projected read should return rows", response.getRecords().getRowCount() > 0);
    }

    @Test
    public void doReadRecords_WhenNonZeroSplitIndex_ReturnsReadRecordsResponse()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("c_customer_id", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                .add("AAAAAAAABAAAAAAA")
                .add("AAAAAAAACAAAAAAA")
                .add("AAAAAAAADAAAAAAA").build());

        ReadRecordsRequest request = newReadRecordsRequest(schemaForRead, table.getName(),
                newSplit("1", "1000", "1"),
                createConstraints(constraintsMap, Collections.emptyMap()),
                100_000_000_000L,
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertNotNull(response.getRecords());
    }

    private Constraints createConstraints(
            Map<String, ValueSet> summaryConstraints,
            Map<String, String> queryPassthroughArguments)
    {
        return new Constraints(
                summaryConstraints,
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                queryPassthroughArguments,
                null);
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

    private Split newSplit(String splitNumber, String splitTotalNumber, String scaleFactor)
    {
        Split.Builder builder = Split.newBuilder(
                        S3SpillLocation.newBuilder()
                                .withBucket(UUID.randomUUID().toString())
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create())
                .add(SPLIT_NUMBER_FIELD, splitNumber)
                .add(SPLIT_TOTAL_NUMBER_FIELD, splitTotalNumber);
        if (scaleFactor != null) {
            builder.add(SPLIT_SCALE_FACTOR_FIELD, scaleFactor);
        }
        return builder.build();
    }

    private ReadRecordsRequest newReadRecordsRequest(Schema schema, String tableName, Split split,
                                                     Constraints constraints, long maxBlockSize, long maxInlineBlockSize)
    {
        return new ReadRecordsRequest(identity,
                TEST_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(TEST_SCHEMA_TPCDS1, tableName),
                schema,
                split,
                constraints,
                maxBlockSize,
                maxInlineBlockSize);
    }
}

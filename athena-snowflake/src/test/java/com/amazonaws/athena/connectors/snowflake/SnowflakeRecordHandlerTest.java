
/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
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
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
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
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_QUERY_ID;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SnowflakeRecordHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeRecordHandlerTest.class);
    private SnowflakeRecordHandler handler;
    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private Connection connection;
    private static final BufferAllocator bufferAllocator = new RootAllocator();
    private BlockAllocator allocator;
    private S3BlockSpillReader spillReader;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();

    @Before
    public void setup()
            throws Exception
    {
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new SnowflakeQueryStringBuilder(SNOWFLAKE_QUOTE_CHARACTER, new SnowflakeFederationExpressionParser(SNOWFLAKE_QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SnowflakeConstants.SNOWFLAKE_NAME,
                "snowflake://jdbc:snowflake://hostname/?warehouse=warehousename&db=dbname&schema=schemaname&user=xxx&password=xxx");
        Mockito.lenient().when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return PutObjectResponse.builder().build();
                });

        Mockito.lenient().when(amazonS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                });
        allocator = new BlockAllocatorImpl();
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
        this.handler = new SnowflakeRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        logger.info("doReadRecordsNoSpill: enter");

        VectorSchemaRoot schemaRoot = createRoot();
        ArrowReader mockReader = mock(ArrowReader.class);
        when(mockReader.loadNextBatch()).thenReturn(true, false);
        when(mockReader.getVectorSchemaRoot()).thenReturn(schemaRoot);
        SnowflakeRecordHandler handlerSpy = spy(handler);
        doReturn(mockReader).when(handlerSpy).constructArrowReader(any());

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("time", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 100L)), false));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(SNOWFLAKE_SPLIT_QUERY_ID, "query_id")
                .add(SNOWFLAKE_SPLIT_EXPORT_BUCKET, "export_bucket")
                .add(SNOWFLAKE_SPLIT_OBJECT_KEY, "s3_object_key");

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
                DEFAULT_CATALOG,
                QUERY_ID,
                TABLE_NAME,
                schemaRoot.getSchema(),
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                100_000_000_000L,
                100_000_000_000L//100GB don't expect this to spill
        );
        RecordResponse rawResponse = handlerSpy.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() == 2);
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 1));

        for (Field field : schemaRoot.getSchema().getFields()) {
            assertTrue(response.getRecords().getFieldVector(field.getName()).getObject(0).equals(schemaRoot.getVector(field).getObject(0)));
            assertTrue(response.getRecords().getFieldVector(field.getName()).getObject(1).equals(schemaRoot.getVector(field).getObject(1)));
        }

        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");

        VectorSchemaRoot schemaRoot = createRoot();
        ArrowReader mockReader = mock(ArrowReader.class);
        when(mockReader.loadNextBatch()).thenReturn(true, false);
        when(mockReader.getVectorSchemaRoot()).thenReturn(schemaRoot);
        SnowflakeRecordHandler handlerSpy = spy(handler);
        doReturn(mockReader).when(handlerSpy).constructArrowReader(any());

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("time", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 100L)), false));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(SNOWFLAKE_SPLIT_QUERY_ID, "query_id")
                .add(SNOWFLAKE_SPLIT_EXPORT_BUCKET, "export_bucket")
                .add(SNOWFLAKE_SPLIT_OBJECT_KEY, "s3_object_key");

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
                DEFAULT_CATALOG,
                QUERY_ID,
                TABLE_NAME,
                schemaRoot.getSchema(),
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                1_500_000L, //~1.5MB so we should see some spill
                0L
        );
        RecordResponse rawResponse = handlerSpy.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            //assertTrue(response.getNumberBlocks() > 1);

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

    private VectorSchemaRoot createRoot()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addBigIntField("day")
                .addBigIntField("month")
                .addBigIntField("year")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();
        VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, bufferAllocator);
        BigIntVector dayVector = (BigIntVector) schemaRoot.getVector("day");
        dayVector.allocateNew(2);
        dayVector.set(0, 0);
        dayVector.set(1, 1);
        dayVector.setValueCount(2);
        BigIntVector monthVector = (BigIntVector) schemaRoot.getVector("month");
        monthVector.allocateNew(2);
        monthVector.set(0, 0);
        monthVector.set(1, 1);
        monthVector.setValueCount(2);
        BigIntVector yearVector = (BigIntVector) schemaRoot.getVector("year");
        yearVector.allocateNew(2);
        yearVector.set(0, 2000);
        yearVector.set(1, 2001);
        yearVector.setValueCount(2);
        VarCharVector stmtVector = (VarCharVector) schemaRoot.getVector("preparedStmt");
        stmtVector.allocateNew(2);
        stmtVector.set(0, new Text("test1"));
        stmtVector.set(1, new Text("test2"));
        stmtVector.setValueCount(2);
        VarCharVector idVector = (VarCharVector) schemaRoot.getVector("queryId");
        idVector.allocateNew(2);
        idVector.set(0, new Text("queryID1"));
        idVector.set(1, new Text("queryID2"));
        idVector.setValueCount(2);
        VarCharVector regionVector = (VarCharVector) schemaRoot.getVector("awsRegionSql");
        regionVector.allocateNew(2);
        regionVector.set(0, new Text("region1"));
        regionVector.set(1, new Text("region2"));
        regionVector.setValueCount(2);
        schemaRoot.setRowCount(2);
        return schemaRoot;
    }
}

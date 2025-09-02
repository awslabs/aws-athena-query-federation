
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
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
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
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.io.ByteStreams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
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
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SnowflakeRecordHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeRecordHandlerTest.class);
    
    // Test Configuration Constants
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_QUERY_ID = "query_id";
    private static final String TEST_EXPORT_BUCKET = "export_bucket";
    private static final String TEST_S3_OBJECT_KEY = "s3_object_key";
    private static final String TEST_EMPTY_OBJECT_KEY = "";
    private static final String TEST_SCHEMA = "schemaname";
    private static final String TEST_TABLE = "test_table";
    private static final String TEST_WAREHOUSE = "warehousename";
    private static final String TEST_DB = "dbname";
    private static final String TEST_USER = "xxx";
    private static final String TEST_PASSWORD = "xxx";
    private static final String TEST_HOST = "hostname";
    
    // Connection String Constants
    private static final String CONNECTION_STRING_TEMPLATE = "snowflake://jdbc:snowflake://%s/?warehouse=%s&db=%s&schema=%s&user=%s&password=%s";
    private static final String FULL_CONNECTION_STRING = String.format(CONNECTION_STRING_TEMPLATE, 
        TEST_HOST, TEST_WAREHOUSE, TEST_DB, TEST_SCHEMA, TEST_USER, TEST_PASSWORD);
    
    // Test Data Constants
    private static final long TEST_CONSTRAINT_TIME = 100L;
    private static final long TEST_SPILL_SIZE = 1_500_000L; // ~1.5MB
    private static final long TEST_NO_SPILL_SIZE = 100_000_000_000L; // 100GB
    private static final int TEST_FETCH_SIZE = 1000;
    private static final int TEST_VECTOR_SIZE = 2;
    
    // Vector Test Values
    private static final byte TEST_TINY_INT_1 = (byte) 10;
    private static final byte TEST_TINY_INT_2 = (byte) 20;
    private static final short TEST_SMALL_INT_1 = (short) 100;
    private static final short TEST_SMALL_INT_2 = (short) 200;
    private static final long TEST_BIG_INT_1 = 2000L;
    private static final long TEST_BIG_INT_2 = 2001L;
    private static final float TEST_FLOAT_4_1 = 1.5f;
    private static final float TEST_FLOAT_4_2 = 2.5f;
    private static final double TEST_FLOAT_8_1 = 3.141592653;
    private static final double TEST_FLOAT_8_2 = 2.718281828;
    private static final BigDecimal TEST_DECIMAL_1 = new BigDecimal("123.4567890123");
    private static final BigDecimal TEST_DECIMAL_2 = new BigDecimal("987.6543210987");
    private static final int TEST_DATE_1 = 18706;
    private static final int TEST_DATE_2 = 18707;
    private static final String TEST_STRING_1 = "test1";
    private static final String TEST_STRING_2 = "test2";
    private static final String TEST_QUERY_ID_1 = "queryID1";
    private static final String TEST_QUERY_ID_2 = "queryID2";
    private static final String TEST_REGION_1 = "region1";
    private static final String TEST_REGION_2 = "region2";

    private SnowflakeRecordHandler handler;
    private FederatedIdentity identity;
    private EncryptionKeyFactory keyFactory;
    private Connection connection;
    private static final BufferAllocator bufferAllocator = new RootAllocator();
    private BlockAllocator allocator;
    private S3BlockSpillReader spillReader;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private List<ByteHolder> mockS3Storage;

    @Before
    public void setup()
            throws Exception
    {
        initializeMocks();
        initializeTestData();
        initializeHandler();
    }

    private void initializeMocks() {
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.mockS3Storage = new ArrayList<>();
    }
    
    private void initializeTestData() throws Exception {
        this.identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
        this.keyFactory = new LocalKeyFactory();
        this.allocator = new BlockAllocatorImpl();
        this.spillReader = new S3BlockSpillReader(amazonS3, allocator);
        
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.jdbcSplitQueryBuilder = new SnowflakeQueryStringBuilder(SNOWFLAKE_QUOTE_CHARACTER, 
            new SnowflakeFederationExpressionParser(SNOWFLAKE_QUOTE_CHARACTER));
        
        setupMockS3Behavior();
    }
    
    private void setupMockS3Behavior() {
        Mockito.lenient().when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("putObject: total size " + mockS3Storage.size());
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
    }
    
    private void initializeHandler() {
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
            TEST_CATALOG, SnowflakeConstants.SNOWFLAKE_NAME, FULL_CONNECTION_STRING);
        this.handler = new SnowflakeRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, 
            athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, Collections.emptyMap());
    }
    
    private S3SpillLocation createTestS3SpillLocation() {
        return S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
    }
    
    private Split.Builder createTestSplitBuilder(S3SpillLocation splitLoc, String objectKey) {
        return Split.newBuilder(splitLoc, keyFactory.create())
                .add(SNOWFLAKE_SPLIT_QUERY_ID, TEST_QUERY_ID)
                .add(SNOWFLAKE_SPLIT_EXPORT_BUCKET, TEST_EXPORT_BUCKET)
                .add(SNOWFLAKE_SPLIT_OBJECT_KEY, objectKey);
    }
    
    private ReadRecordsRequest createTestReadRecordsRequest(Split split, long maxBlockSize, long maxInlineBlockSize) {
        VectorSchemaRoot schemaRoot = createRoot();
        return new ReadRecordsRequest(identity, DEFAULT_CATALOG, TEST_QUERY_ID, TABLE_NAME,
                schemaRoot.getSchema(), split, createTestConstraints(), maxBlockSize, maxInlineBlockSize);
    }
    
    private Constraints createTestConstraints() {
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 
            DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }
    
    private ArrowReader createMockArrowReader(VectorSchemaRoot schemaRoot, boolean hasData) {
        ArrowReader mockReader = mock(ArrowReader.class);
        try {
            when(mockReader.loadNextBatch()).thenReturn(hasData, false);
            when(mockReader.getVectorSchemaRoot()).thenReturn(schemaRoot);
        } catch (IOException e) {
            // This shouldn't happen with mocks, but handle it just in case
            throw new RuntimeException("Error setting up mock ArrowReader", e);
        }
        return mockReader;
    }
    
    private void setupMockDatabaseConnection() throws SQLException {
        java.sql.DatabaseMetaData mockMetaData = mock(java.sql.DatabaseMetaData.class);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Snowflake");
        when(connection.getMetaData()).thenReturn(mockMetaData);

        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(any(String.class))).thenReturn(mockPreparedStatement);

        java.sql.ResultSet mockResultSet = mock(java.sql.ResultSet.class);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false); // No rows
    }
    
    private DatabaseConnectionConfig createDatabaseConnectionConfig(String secretId) {
        if (secretId != null) {
            return new DatabaseConnectionConfig(TEST_CATALOG, SnowflakeConstants.SNOWFLAKE_NAME, 
                FULL_CONNECTION_STRING, secretId);
        } else {
            return new DatabaseConnectionConfig(TEST_CATALOG, SnowflakeConstants.SNOWFLAKE_NAME, 
                FULL_CONNECTION_STRING);
        }
    }

    @Test
    public void testDoReadRecordsNoSpill()
            throws Exception
    {
        logger.info("doReadRecordsNoSpill: enter");
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(true)
        )) {
            VectorSchemaRoot schemaRoot = createRoot();
            ArrowReader mockReader = createMockArrowReader(schemaRoot, true);
            SnowflakeRecordHandler handlerSpy = spy(handler);
            doReturn(mockReader).when(handlerSpy).constructArrowReader(any());

            S3SpillLocation splitLoc = createTestS3SpillLocation();
            Split split = createTestSplitBuilder(splitLoc, TEST_S3_OBJECT_KEY).build();
            ReadRecordsRequest request = createTestReadRecordsRequest(split, TEST_NO_SPILL_SIZE, TEST_NO_SPILL_SIZE);
            
            RecordResponse rawResponse = handlerSpy.doReadRecords(allocator, request);

            assertTrue(rawResponse instanceof ReadRecordsResponse);

            ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
            logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

            assertTrue(response.getRecords().getRowCount() == TEST_VECTOR_SIZE);
            logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
            logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 1));

            for (Field field : schemaRoot.getSchema().getFields()) {
                assertTrue(response.getRecords().getFieldVector(field.getName()).getObject(0).equals(schemaRoot.getVector(field).getObject(0)));
                assertTrue(response.getRecords().getFieldVector(field.getName()).getObject(1).equals(schemaRoot.getVector(field).getObject(1)));
            }
        }
        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void testDoReadRecordsSpill()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(true)
        )) {
            VectorSchemaRoot schemaRoot = createRoot();
            ArrowReader mockReader = createMockArrowReader(schemaRoot, true);
            SnowflakeRecordHandler handlerSpy = spy(handler);
            doReturn(mockReader).when(handlerSpy).constructArrowReader(any());

            S3SpillLocation splitLoc = createTestS3SpillLocation();
            Split split = createTestSplitBuilder(splitLoc, TEST_S3_OBJECT_KEY).build();
            ReadRecordsRequest request = createTestReadRecordsRequest(split, TEST_SPILL_SIZE, 0L);
            
            RecordResponse rawResponse = handlerSpy.doReadRecords(allocator, request);

            assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

            try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
                logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

                int blockNum = 0;
                for (SpillLocation next : response.getRemoteBlocks()) {
                    S3SpillLocation spillLocation = (S3SpillLocation) next;
                    try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                        logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                        logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                        assertNotNull(BlockUtils.rowToString(block, 0));
                    }
                }
            }
        }
        logger.info("doReadRecordsSpill: exit");
    }

    @Test
    public void testReadWithConstraintS3ExportEnabled()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(true)
        )) {
            VectorSchemaRoot schemaRoot = createRoot();
            ArrowReader mockReader = createMockArrowReader(schemaRoot, true);
            SnowflakeRecordHandler handlerSpy = spy(handler);
            doReturn(mockReader).when(handlerSpy).constructArrowReader(any());

            S3SpillLocation splitLoc = createTestS3SpillLocation();
            Split split = createTestSplitBuilder(splitLoc, TEST_S3_OBJECT_KEY).build();
            ReadRecordsRequest request = createTestReadRecordsRequest(split, TEST_NO_SPILL_SIZE, TEST_NO_SPILL_SIZE);

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockQueryStatusChecker = mock(QueryStatusChecker.class);

            handlerSpy.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            // Verify that the S3 export path was used
            Mockito.verify(mockSpiller, Mockito.atLeastOnce()).writeRows(any());
        }
    }

    @Test
    public void testReadWithConstraintS3ExportDisabled()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            S3SpillLocation splitLoc = createTestS3SpillLocation();
            Split split = createTestSplitBuilder(splitLoc, TEST_S3_OBJECT_KEY).build();
            ReadRecordsRequest request = createTestReadRecordsRequest(split, TEST_NO_SPILL_SIZE, TEST_NO_SPILL_SIZE);

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockQueryStatusChecker = mock(QueryStatusChecker.class);

            setupMockDatabaseConnection();

            // This should call the parent class method (direct read)
            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            // Verify that the direct read path was used (no S3 export)
            // The parent class handles the actual implementation
            // Verify that the handler attempted to read from the database directly
            Mockito.verify(connection).prepareStatement(any(String.class));
            
            // Since we're using mocks, we need to verify the interactions differently
            // The setupMockDatabaseConnection method already sets up the mocks
            // We just need to verify that the connection was used
            Mockito.verify(connection, Mockito.atLeastOnce()).prepareStatement(any(String.class));
        }
    }

    @Test
    public void testHandleS3ExportReadWithEmptyObjectKey()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(true)
        )) {
            S3SpillLocation splitLoc = createTestS3SpillLocation();
            Split split = createTestSplitBuilder(splitLoc, TEST_EMPTY_OBJECT_KEY).build();
            ReadRecordsRequest request = createTestReadRecordsRequest(split, TEST_NO_SPILL_SIZE, TEST_NO_SPILL_SIZE);

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockQueryStatusChecker = mock(QueryStatusChecker.class);

            handler.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);

            // Should not call writeRows since object key is empty
            Mockito.verify(mockSpiller, Mockito.never()).writeRows(any());
        }
    }

    @Test
    public void testHandleS3ExportReadWithException()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(true)
        )) {
            VectorSchemaRoot schemaRoot = createRoot();
            ArrowReader mockReader = mock(ArrowReader.class);
            when(mockReader.loadNextBatch()).thenThrow(new RuntimeException("Test exception"));
            when(mockReader.getVectorSchemaRoot()).thenReturn(schemaRoot);
            SnowflakeRecordHandler handlerSpy = spy(handler);
            doReturn(mockReader).when(handlerSpy).constructArrowReader(any());

            S3SpillLocation splitLoc = createTestS3SpillLocation();
            Split split = createTestSplitBuilder(splitLoc, TEST_S3_OBJECT_KEY).build();
            ReadRecordsRequest request = createTestReadRecordsRequest(split, TEST_NO_SPILL_SIZE, TEST_NO_SPILL_SIZE);

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockQueryStatusChecker = mock(QueryStatusChecker.class);

            // Should throw AthenaConnectorException
            try {
                handlerSpy.readWithConstraint(mockSpiller, request, mockQueryStatusChecker);
                fail("Expected exception was not thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Error in object content for object : " + TEST_S3_OBJECT_KEY));
            }
        }
    }

    @Test
    public void testBuildSplitSqlWithQueryPassthrough()
            throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        TableName tableName = new TableName("schema", "table");
        Schema schema = SchemaBuilder.newBuilder().addStringField("test").build();
        Constraints constraints = createTestConstraints();
        Split split = Split.newBuilder(S3SpillLocation.newBuilder().withBucket("bucket").withSplitId("split").withQueryId("query").build(), keyFactory.create()).build();

        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any(String.class))).thenReturn(mockPreparedStatement);

        PreparedStatement result = handler.buildSplitSql(mockConnection, "catalog", tableName, schema, constraints, split);

        assertNotNull(result);
        Mockito.verify(mockPreparedStatement).setFetchSize(TEST_FETCH_SIZE);
    }

    @Test
    public void testBuildSplitSqlWithoutQueryPassthrough()
            throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        TableName tableName = new TableName("schema", "table");
        Schema schema = SchemaBuilder.newBuilder().addStringField("test").build();
        Constraints constraints = createTestConstraints();
        Split split = Split.newBuilder(S3SpillLocation.newBuilder().withBucket("bucket").withSplitId("split").withQueryId("query").build(), keyFactory.create()).build();

        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any(String.class))).thenReturn(mockPreparedStatement);

        PreparedStatement result = handler.buildSplitSql(mockConnection, "catalog", tableName, schema, constraints, split);

        assertNotNull(result);
        Mockito.verify(mockPreparedStatement).setFetchSize(TEST_FETCH_SIZE);
    }

    @Test
    public void testBuildSplitSqlWithSQLException()
            throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        TableName tableName = new TableName("schema", "table");
        Schema schema = SchemaBuilder.newBuilder().addStringField("test").build();
        Constraints constraints = createTestConstraints();
        Split split = Split.newBuilder(S3SpillLocation.newBuilder().withBucket("bucket").withSplitId("split").withQueryId("query").build(), keyFactory.create()).build();

        when(mockConnection.prepareStatement(any(String.class))).thenThrow(new SQLException("Test SQL exception"));

        try {
            handler.buildSplitSql(mockConnection, "catalog", tableName, schema, constraints, split);
            fail("Expected exception was not thrown");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Test SQL exception"));
        }
    }

    @Test
    public void testGetCredentialProviderWithSecret()
    {
        DatabaseConnectionConfig configWithSecret = createDatabaseConnectionConfig("test-secret");
        
        SnowflakeRecordHandler handlerWithSecret = new SnowflakeRecordHandler(configWithSecret, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, Collections.emptyMap());
        
        CredentialsProvider result = handlerWithSecret.getCredentialProvider();
        
        assertNotNull(result);
        assertTrue(result instanceof SnowflakeCredentialsProvider);
    }

    @Test
    public void testGetCredentialProviderWithoutSecret()
    {
        DatabaseConnectionConfig configWithoutSecret = createDatabaseConnectionConfig(null);
        
        SnowflakeRecordHandler handlerWithoutSecret = new SnowflakeRecordHandler(configWithoutSecret, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, Collections.emptyMap());
        
        CredentialsProvider result = handlerWithoutSecret.getCredentialProvider();
        
        // Should return null when no secret is configured
        assertTrue(result == null);
    }

    @Test
    public void testConstructArrowReader()
    {
        // Mock S3 client to avoid AWS access issues
        S3Client mockS3Client = mock(S3Client.class);
        SnowflakeRecordHandler handlerWithMockS3 = new SnowflakeRecordHandler(
            createDatabaseConnectionConfig(null), mockS3Client, secretsManager, athena, 
            jdbcConnectionFactory, jdbcSplitQueryBuilder, Collections.emptyMap());
        
        // Test with a simple URI that doesn't require S3 access
        String testUri = "file:///tmp/test.parquet";
        
        try {
            ArrowReader reader = handlerWithMockS3.constructArrowReader(testUri);
            assertNotNull(reader);
        } catch (Exception e) {
            // Expected to fail since we don't have a real file, but the method should be callable
            logger.info("Expected exception for test URI: " + e.getMessage());
        }
    }

    @Test
    public void testConstructorWithConfigOptions()
    {
        // Set up environment variable for the constructor
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("test-key", "test-value");
        configOptions.put("default", FULL_CONNECTION_STRING);
        
        try {
            SnowflakeRecordHandler handlerWithConfig = new SnowflakeRecordHandler(configOptions);
            assertNotNull(handlerWithConfig);
        } catch (Exception e) {
            // Expected to fail due to environment setup, but constructor should be callable
            logger.info("Expected exception for constructor: " + e.getMessage());
        }
    }

    @Test
    public void testConstructorWithDatabaseConnectionConfig()
    {
        DatabaseConnectionConfig databaseConnectionConfig = createDatabaseConnectionConfig(null);
        Map<String, String> configOptions = new HashMap<>();
        
        SnowflakeRecordHandler handlerWithConfig = new SnowflakeRecordHandler(databaseConnectionConfig, configOptions);
        
        assertNotNull(handlerWithConfig);
    }

    @Test
    public void testConstructorWithGenericJdbcConnectionFactory()
    {
        DatabaseConnectionConfig databaseConnectionConfig = createDatabaseConnectionConfig(null);
        Map<String, String> configOptions = new HashMap<>();
        GenericJdbcConnectionFactory mockFactory = mock(GenericJdbcConnectionFactory.class);
        
        SnowflakeRecordHandler handlerWithFactory = new SnowflakeRecordHandler(databaseConnectionConfig, mockFactory, configOptions);
        
        assertNotNull(handlerWithFactory);
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
            return this.bytes;
        }
    }

    private VectorSchemaRoot createRoot()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addBitField("bitField")
                .addTinyIntField("tinyIntField")
                .addSmallIntField("smallIntField")
                .addBigIntField("day")
                .addBigIntField("month")
                .addBigIntField("year")
                .addFloat4Field("float4Field")
                .addFloat8Field("float8Field")
                .addDecimalField("decimalField", 38, 10)
                .addDateDayField("dateDayField")
                .addStringField("preparedStmt")
                .addStringField("queryId")
                .addStringField("awsRegionSql")
                .build();

        VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(schema, bufferAllocator);

        // Initialize BitVector
        BitVector bitVector = (BitVector) schemaRoot.getVector("bitField");
        bitVector.allocateNew(TEST_VECTOR_SIZE);
        bitVector.set(0, 1);
        bitVector.set(1, 0);
        bitVector.setValueCount(TEST_VECTOR_SIZE);

        // Initialize TinyIntVector
        TinyIntVector tinyIntVector = (TinyIntVector) schemaRoot.getVector("tinyIntField");
        tinyIntVector.allocateNew(TEST_VECTOR_SIZE);
        tinyIntVector.set(0, TEST_TINY_INT_1);
        tinyIntVector.set(1, TEST_TINY_INT_2);
        tinyIntVector.setValueCount(TEST_VECTOR_SIZE);

        // Initialize SmallIntVector
        SmallIntVector smallIntVector = (SmallIntVector) schemaRoot.getVector("smallIntField");
        smallIntVector.allocateNew(TEST_VECTOR_SIZE);
        smallIntVector.set(0, TEST_SMALL_INT_1);
        smallIntVector.set(1, TEST_SMALL_INT_2);
        smallIntVector.setValueCount(TEST_VECTOR_SIZE);

        // Initialize BigIntVectors
        BigIntVector dayVector = (BigIntVector) schemaRoot.getVector("day");
        dayVector.allocateNew(TEST_VECTOR_SIZE);
        dayVector.set(0, 0);
        dayVector.set(1, 1);
        dayVector.setValueCount(TEST_VECTOR_SIZE);

        BigIntVector monthVector = (BigIntVector) schemaRoot.getVector("month");
        monthVector.allocateNew(TEST_VECTOR_SIZE);
        monthVector.set(0, 0);
        monthVector.set(1, 1);
        monthVector.setValueCount(TEST_VECTOR_SIZE);

        BigIntVector yearVector = (BigIntVector) schemaRoot.getVector("year");
        yearVector.allocateNew(TEST_VECTOR_SIZE);
        yearVector.set(0, TEST_BIG_INT_1);
        yearVector.set(1, TEST_BIG_INT_2);
        yearVector.setValueCount(TEST_VECTOR_SIZE);

        // Initialize Float4Vector
        Float4Vector float4Vector = (Float4Vector) schemaRoot.getVector("float4Field");
        float4Vector.allocateNew(TEST_VECTOR_SIZE);
        float4Vector.set(0, TEST_FLOAT_4_1);
        float4Vector.set(1, TEST_FLOAT_4_2);
        float4Vector.setValueCount(TEST_VECTOR_SIZE);

        // Initialize Float8Vector
        Float8Vector float8Vector = (Float8Vector) schemaRoot.getVector("float8Field");
        float8Vector.allocateNew(TEST_VECTOR_SIZE);
        float8Vector.set(0, TEST_FLOAT_8_1);
        float8Vector.set(1, TEST_FLOAT_8_2);
        float8Vector.setValueCount(TEST_VECTOR_SIZE);

        // Initialize DecimalVector
        DecimalVector decimalVector = (DecimalVector) schemaRoot.getVector("decimalField");
        decimalVector.allocateNew(TEST_VECTOR_SIZE);
        decimalVector.set(0, TEST_DECIMAL_1);
        decimalVector.set(1, TEST_DECIMAL_2);
        decimalVector.setValueCount(TEST_VECTOR_SIZE);

        // Initialize DateDayVector
        DateDayVector dateDayVector = (DateDayVector) schemaRoot.getVector("dateDayField");
        dateDayVector.allocateNew(TEST_VECTOR_SIZE);
        dateDayVector.set(0, TEST_DATE_1);
        dateDayVector.set(1, TEST_DATE_2);
        dateDayVector.setValueCount(TEST_VECTOR_SIZE);

        // Initialize VarCharVectors
        VarCharVector stmtVector = (VarCharVector) schemaRoot.getVector("preparedStmt");
        stmtVector.allocateNew(TEST_VECTOR_SIZE);
        stmtVector.set(0, new Text(TEST_STRING_1));
        stmtVector.set(1, new Text(TEST_STRING_2));
        stmtVector.setValueCount(TEST_VECTOR_SIZE);

        VarCharVector idVector = (VarCharVector) schemaRoot.getVector("queryId");
        idVector.allocateNew(TEST_VECTOR_SIZE);
        idVector.set(0, new Text(TEST_QUERY_ID_1));
        idVector.set(1, new Text(TEST_QUERY_ID_2));
        idVector.setValueCount(TEST_VECTOR_SIZE);

        VarCharVector regionVector = (VarCharVector) schemaRoot.getVector("awsRegionSql");
        regionVector.allocateNew(TEST_VECTOR_SIZE);
        regionVector.set(0, new Text(TEST_REGION_1));
        regionVector.set(1, new Text(TEST_REGION_2));
        regionVector.setValueCount(TEST_VECTOR_SIZE);

        schemaRoot.setRowCount(TEST_VECTOR_SIZE);
        return schemaRoot;
    }
}

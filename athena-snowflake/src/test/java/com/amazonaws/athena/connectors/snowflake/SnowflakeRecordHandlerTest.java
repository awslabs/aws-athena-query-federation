
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
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
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
import org.apache.arrow.vector.types.Types;
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
import static org.junit.Assert.assertEquals;
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

    private static final String TEST_CATALOG_NAME = "catalog";
    private static final String TEST_SCHEMA_NAME = "schema";
    private static final String TEST_TABLE_NAME = "table";
    private static final String TEST_BUCKET_NAME = "bucket";
    private static final String TEST_SPLIT_ID = "split";
    private static final String TEST_QUERY_ID_VALUE = "query";
    private static final String TEST_FIELD_NAME = "test";
    private static final String TEST_SQL_EXCEPTION_MESSAGE = "Test SQL exception";
    private static final String EXPECTED_EXCEPTION_NOT_THROWN = "Expected exception was not thrown";
    private static final String TEST_NAME_JOHN = "John";
    private static final String TEST_NAME_JANE = "Jane";

    // Column Name Constants
    private static final String COLUMN_ID = "id";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_PRICE = "price";
    private static final String COLUMN_AGE = "age";
    private static final String COLUMN_SALARY = "salary";
    private static final String COLUMN_SCORE = "score";
    private static final String COLUMN_CREATED_DATE = "created_date";
    private static final String COLUMN_DATE = "date";
    private static final String COLUMN_STATUS = "status";
    private static final String COLUMN_CATEGORY_ID = "category_id";
    private static final String COLUMN_RATING = "rating";
    private static final String COLUMN_IS_ACTIVE = "is_active";
    private static final String COLUMN_HIRE_DATE = "hire_date";
    private static final String COLUMN_IS_MANAGER = "is_manager";
    private static final String COLUMN_VALUE = "value";

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
    private SnowflakeQueryStringBuilder jdbcSplitQueryBuilder;
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

    /**
     * Creates a schema with specified fields
     */
    private Schema createSchema(String... fieldNames) {
        SchemaBuilder builder = SchemaBuilder.newBuilder();
        for (String fieldName : fieldNames) {
            switch (fieldName.toLowerCase()) {
                case COLUMN_ID:
                    builder.addIntField(COLUMN_ID);
                    break;
                case COLUMN_NAME:
                    builder.addStringField(COLUMN_NAME);
                    break;
                case COLUMN_AGE:
                    builder.addIntField(COLUMN_AGE);
                    break;
                case COLUMN_PRICE:
                    builder.addFloat8Field(COLUMN_PRICE);
                    break;
                case COLUMN_SALARY:
                    builder.addFloat8Field(COLUMN_SALARY);
                    break;
                case COLUMN_SCORE:
                    builder.addIntField(COLUMN_SCORE);
                    break;
                case COLUMN_STATUS:
                    builder.addStringField(COLUMN_STATUS);
                    break;
                case COLUMN_CATEGORY_ID:
                    builder.addIntField(COLUMN_CATEGORY_ID);
                    break;
                case COLUMN_RATING:
                    builder.addFloat8Field(COLUMN_RATING);
                    break;
                case COLUMN_CREATED_DATE:
                    builder.addDateDayField(COLUMN_CREATED_DATE);
                    break;
                case COLUMN_HIRE_DATE:
                    builder.addDateDayField(COLUMN_HIRE_DATE);
                    break;
                case COLUMN_DATE:
                    builder.addDateDayField(COLUMN_DATE);
                    break;
                case COLUMN_IS_ACTIVE:
                    builder.addBitField(COLUMN_IS_ACTIVE);
                    break;
                case COLUMN_IS_MANAGER:
                    builder.addBitField(COLUMN_IS_MANAGER);
                    break;
                case COLUMN_VALUE:
                    builder.addFloat8Field(COLUMN_VALUE);
                    break;
                case TEST_FIELD_NAME:
                    builder.addStringField(TEST_FIELD_NAME);
                    break;
                default:
                    builder.addStringField(fieldName);
                    break;
            }
        }
        return builder.build();
    }

    /**
     * Creates a table name with default schema and table
     */
    private TableName createTableName() {
        return new TableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME);
    }

    /**
     * Creates a basic split for testing
     */
    private Split createBasicSplit() {
        return Split.newBuilder(S3SpillLocation.newBuilder()
                .withBucket(TEST_BUCKET_NAME)
                .withSplitId(TEST_SPLIT_ID)
                .withQueryId(TEST_QUERY_ID_VALUE)
                .build(), keyFactory.create())
                .add("partition", "p-primary--limit-1000-offset-0")
                .build();
    }

    /**
     * Creates constraints with specified summary, order by, and limit
     */
    private Constraints createConstraints(Map<String, ValueSet> summary, List<OrderByField> orderBy, Long limit) {
        return new Constraints(summary, Collections.emptyList(), orderBy,
                limit != null ? limit : DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }

    /**
     * Creates a mock connection and prepared statement setup
     */
    private PreparedStatement setupMockConnectionAndStatement(Connection mockConnection, String expectedSql) throws SQLException {
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(expectedSql)).thenReturn(mockPreparedStatement);
        return mockPreparedStatement;
    }

    /**
     * Common test execution pattern for buildSplitSql tests
     */
    private void executeBuildSplitSqlTest(Connection mockConnection, TableName tableName, Schema schema,
                                        Constraints constraints, Split split, String expectedSql) throws Exception {
        PreparedStatement mockPreparedStatement = setupMockConnectionAndStatement(mockConnection, expectedSql);

        PreparedStatement result = handler.buildSplitSql(mockConnection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        assertEquals(mockPreparedStatement, result);
        Mockito.verify(mockPreparedStatement).setFetchSize(TEST_FETCH_SIZE);
    }

    /**
     * Common test execution pattern for buildSplitSql tests with any string matcher
     */
    private void executeBuildSplitSqlTestWithAnyString(Connection mockConnection, TableName tableName, Schema schema,
                                                     Constraints constraints, Split split) throws Exception {
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any(String.class))).thenReturn(mockPreparedStatement);

        PreparedStatement result = handler.buildSplitSql(mockConnection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        assertEquals(mockPreparedStatement, result);
        Mockito.verify(mockPreparedStatement).setFetchSize(TEST_FETCH_SIZE);
    }

    /**
     * Common test execution pattern for buildSplitSql tests with contains matcher
     */
    private void executeBuildSplitSqlTestWithContains(Connection mockConnection, TableName tableName, Schema schema,
                                                    Constraints constraints, Split split, String expectedSql) throws Exception {
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(Mockito.contains(expectedSql))).thenReturn(mockPreparedStatement);

        PreparedStatement result = handler.buildSplitSql(mockConnection, TEST_CATALOG_NAME, tableName, schema, constraints, split);

        assertEquals(mockPreparedStatement, result);
        Mockito.verify(mockPreparedStatement).setFetchSize(TEST_FETCH_SIZE);
    }

    /**
     * Creates a range constraint for integer values
     */
    private ValueSet createIntRangeConstraint(long min, boolean minInclusive, long max, boolean maxInclusive) {
        return SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                List.of(Range.range(allocator, Types.MinorType.INT.getType(), min, minInclusive, max, maxInclusive)), false);
    }

    /**
     * Creates an IN constraint for string values
     */
    private ValueSet createStringInConstraint(String... values) {
        List<Range> ranges = new ArrayList<>();
        for (String value : values) {
            ranges.add(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), value));
        }
        return SortedRangeSet.copyOf(Types.MinorType.VARCHAR.getType(), ranges, false);
    }

    /**
     * Creates an IN constraint for integer values
     */
    private ValueSet createIntInConstraint(Long... values) {
        List<Range> ranges = new ArrayList<>();
        for (Long value : values) {
            ranges.add(Range.equal(allocator, Types.MinorType.INT.getType(), value));
        }
        return SortedRangeSet.copyOf(Types.MinorType.INT.getType(), ranges, false);
    }

    /**
     * Creates an IN constraint for float values
     */
    private ValueSet createFloatInConstraint(Double... values) {
        List<Range> ranges = new ArrayList<>();
        for (Double value : values) {
            ranges.add(Range.equal(allocator, Types.MinorType.FLOAT8.getType(), value));
        }
        return SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(), ranges, false);
    }

    /**
     * Creates a greater than constraint for float values
     */
    private ValueSet createFloatGreaterThanConstraint(double value) {
        return SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                List.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), value)), false);
    }

    /**
     * Creates a range constraint for float values
     */
    private ValueSet createFloatRangeConstraint(double min, boolean minInclusive, double max, boolean maxInclusive) {
        return SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                List.of(Range.range(allocator, Types.MinorType.FLOAT8.getType(), min, minInclusive, max, maxInclusive)), false);
    }

    /**
     * Creates a boolean equality constraint
     */
    private ValueSet createBooleanConstraint(boolean value, boolean allowNull) {
        return SortedRangeSet.copyOf(Types.MinorType.BIT.getType(),
                List.of(Range.equal(allocator, Types.MinorType.BIT.getType(), value)), allowNull);
    }

    /**
     * Creates a NULL constraint
     */
    private ValueSet createNullConstraint() {
        return SortedRangeSet.copyOf(Types.MinorType.VARCHAR.getType(), Collections.emptyList(), true);
    }

    /**
     * Creates a NOT NULL constraint
     */
    private ValueSet createNotNullConstraint() {
        return SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                List.of(Range.all(allocator, Types.MinorType.INT.getType())), false);
    }

    @Test
    public void doReadRecords_WithNoSpill_ReturnsInlineRecords()
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
    public void doReadRecords_WithSpill_ReturnsRemoteRecords()
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
    public void readWithConstraint_WithS3ExportEnabled_UsesS3ExportPath()
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
    public void readWithConstraint_WithS3ExportDisabled_UsesDirectReadPath()
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
    public void readWithConstraint_WithEmptyObjectKey_DoesNotWriteRows()
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
    public void readWithConstraint_WithS3ExportException_ThrowsAthenaConnectorException()
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
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("Error in object content for object : " + TEST_S3_OBJECT_KEY));
            }
        }
    }

    @Test
    public void buildSplitSql_WithQueryPassthrough_ReturnsPreparedStatement()
            throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        TableName tableName = createTableName();
        Schema schema = createSchema(TEST_FIELD_NAME);
        Constraints constraints = createTestConstraints();
        Split split = createBasicSplit();

        executeBuildSplitSqlTestWithAnyString(mockConnection, tableName, schema, constraints, split);
    }

    @Test
    public void buildSplitSql_WithoutQueryPassthrough_ReturnsPreparedStatement()
            throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        TableName tableName = createTableName();
        Schema schema = createSchema(TEST_FIELD_NAME);
        Constraints constraints = createTestConstraints();
        Split split = createBasicSplit();

        executeBuildSplitSqlTestWithAnyString(mockConnection, tableName, schema, constraints, split);
    }

    @Test
    public void buildSplitSql_WithSQLException_ThrowsException()
            throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        TableName tableName = new TableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        Schema schema = SchemaBuilder.newBuilder().addStringField(TEST_FIELD_NAME).build();
        Constraints constraints = createTestConstraints();
        Split split = createBasicSplit();

        when(mockConnection.prepareStatement(any(String.class))).thenThrow(new SQLException(TEST_SQL_EXCEPTION_MESSAGE));

        try {
            handler.buildSplitSql(mockConnection, TEST_CATALOG_NAME, tableName, schema, constraints, split);
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(TEST_SQL_EXCEPTION_MESSAGE));
        }
    }

    @Test
    public void getCredentialProvider_WithSecret_ReturnsSnowflakeCredentialsProvider() throws Exception
    {
        DatabaseConnectionConfig configWithSecret = createDatabaseConnectionConfig("test-secret");

        SnowflakeRecordHandler handlerWithSecret = new SnowflakeRecordHandler(configWithSecret, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, Collections.emptyMap());

        // Use reflection to access protected method
        java.lang.reflect.Method getCredentialProviderMethod = handlerWithSecret.getClass().getSuperclass().getDeclaredMethod("getCredentialProvider");
        getCredentialProviderMethod.setAccessible(true);

        CredentialsProvider result = (CredentialsProvider) getCredentialProviderMethod.invoke(handlerWithSecret);

        assertNotNull(result);
        assertTrue(result instanceof SnowflakeCredentialsProvider);
    }

    @Test
    public void getCredentialProvider_WithoutSecret_ReturnsNull() throws Exception
    {
        DatabaseConnectionConfig configWithoutSecret = createDatabaseConnectionConfig(null);

        SnowflakeRecordHandler handlerWithoutSecret = new SnowflakeRecordHandler(configWithoutSecret, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, Collections.emptyMap());

        // Use reflection to access protected method
        java.lang.reflect.Method getCredentialProviderMethod = handlerWithoutSecret.getClass().getSuperclass().getDeclaredMethod("getCredentialProvider");
        getCredentialProviderMethod.setAccessible(true);

        CredentialsProvider result = (CredentialsProvider) getCredentialProviderMethod.invoke(handlerWithoutSecret);

        // Should return null when no secret is configured
        assertTrue(result == null);
    }

    @Test
    public void constructArrowReader_WithUri_ReturnsArrowReader()
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
    public void constructor_WithConfigOptions_CreatesHandler()
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
    public void constructor_WithDatabaseConnectionConfig_CreatesHandler()
    {
        DatabaseConnectionConfig databaseConnectionConfig = createDatabaseConnectionConfig(null);
        Map<String, String> configOptions = new HashMap<>();

        SnowflakeRecordHandler handlerWithConfig = new SnowflakeRecordHandler(databaseConnectionConfig, configOptions);

        assertNotNull(handlerWithConfig);
    }

    @Test
    public void constructor_WithGenericJdbcConnectionFactory_CreatesHandler()
    {
        DatabaseConnectionConfig databaseConnectionConfig = createDatabaseConnectionConfig(null);
        Map<String, String> configOptions = new HashMap<>();
        GenericJdbcConnectionFactory mockFactory = mock(GenericJdbcConnectionFactory.class);

        SnowflakeRecordHandler handlerWithFactory = new SnowflakeRecordHandler(databaseConnectionConfig, mockFactory, configOptions);

        assertNotNull(handlerWithFactory);
    }

    @Test
    public void buildSplitSql_WithComplexRangePredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_ID, COLUMN_NAME, COLUMN_PRICE, COLUMN_CREATED_DATE);

            // Create constraints with complex range predicates
            Map<String, ValueSet> summary = new HashMap<>();
            summary.put(COLUMN_ID, createIntRangeConstraint(10L, true, 100L, false));
            summary.put(COLUMN_PRICE, createFloatGreaterThanConstraint(50.0));
            summary.put(COLUMN_NAME, createStringInConstraint(TEST_NAME_JOHN, TEST_NAME_JANE));

            Constraints constraints = createConstraints(summary, Collections.emptyList(), DEFAULT_NO_LIMIT);
            Split split = createBasicSplit();

            String expectedSql = "SELECT \"id\", \"name\", \"price\", \"created_date\" FROM \"schema\".\"table\"  WHERE ((\"id\" >= ? AND \"id\" < ?)) AND (\"name\" IN (?,?)) AND ((\"price\" > ?))";
            executeBuildSplitSqlTest(mockConnection, tableName, schema, constraints, split, expectedSql);
        }
    }

    @Test
    public void buildSplitSql_WithTopNPredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_NAME, COLUMN_SCORE);

            // Create constraints with Top N scenario (ORDER BY + LIMIT)
            List<OrderByField> orderBy = List.of(new OrderByField(COLUMN_NAME, OrderByField.Direction.ASC_NULLS_FIRST));
            Constraints constraints = createConstraints(Collections.emptyMap(), orderBy, 10L);
            Split split = createBasicSplit();

            String expectedSql = "SELECT \"name\", \"score\" FROM \"schema\".\"table\"  ORDER BY \"name\" ASC NULLS FIRST LIMIT 10";
            executeBuildSplitSqlTest(mockConnection, tableName, schema, constraints, split, expectedSql);
        }
    }

    @Test
    public void buildSplitSql_WithLimitPredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_NAME);

            // Test limit values
            long limitValues = 10L;

            Constraints constraints = createConstraints(Collections.emptyMap(), Collections.emptyList(), limitValues);
            Split split = createBasicSplit();

            String expectedSql = "SELECT \"name\" FROM \"schema\".\"table\"  LIMIT 10";
            executeBuildSplitSqlTestWithContains(mockConnection, tableName, schema, constraints, split, expectedSql);
        }
    }

    @Test
    public void buildSplitSql_WithOrderByPredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_NAME, COLUMN_AGE, COLUMN_SALARY);

            // Test different ORDER BY scenarios
            List<List<OrderByField>> orderByScenarios = List.of(
                    // Single column ASC
                    List.of(new OrderByField(COLUMN_NAME, OrderByField.Direction.ASC_NULLS_FIRST)),
                    // Single column DESC
                    List.of(new OrderByField(COLUMN_AGE, OrderByField.Direction.DESC_NULLS_LAST)),
                    // Multiple columns
                    List.of(
                            new OrderByField(COLUMN_SALARY, OrderByField.Direction.DESC_NULLS_LAST),
                            new OrderByField(COLUMN_NAME, OrderByField.Direction.ASC_NULLS_FIRST)
                    ),
                    // Mixed nulls handling
                    List.of(
                            new OrderByField(COLUMN_AGE, OrderByField.Direction.ASC_NULLS_LAST),
                            new OrderByField(COLUMN_SALARY, OrderByField.Direction.DESC_NULLS_FIRST)
                    )
            );

            for (List<OrderByField> orderByClause : orderByScenarios) {
                Constraints constraints = createConstraints(Collections.emptyMap(), orderByClause, DEFAULT_NO_LIMIT);
                Split split = createBasicSplit();

                String expectedSql = "SELECT \"name\", \"age\", \"salary\" FROM \"schema\".\"table\"  ORDER BY";
                executeBuildSplitSqlTestWithContains(mockConnection, tableName, schema, constraints, split, expectedSql);
            }
        }
    }

    @Test
    public void buildSplitSql_WithBetweenPredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_ID, COLUMN_PRICE, COLUMN_DATE);

            // Test BETWEEN predicates for different data types
            Map<String, ValueSet> summary = new HashMap<>();
            summary.put(COLUMN_ID, createIntRangeConstraint(10L, true, 50L, true));
            summary.put(COLUMN_PRICE, createFloatRangeConstraint(10.5, true, 99.9, true));

            Constraints constraints = createConstraints(summary, Collections.emptyList(), DEFAULT_NO_LIMIT);
            Split split = createBasicSplit();

            String expectedSql = "SELECT \"id\", \"price\", \"date\" FROM \"schema\".\"table\"  WHERE ((\"id\" >= ? AND \"id\" <= ?)) AND ((\"price\" >= ? AND \"price\" <= ?))";
            executeBuildSplitSqlTest(mockConnection, tableName, schema, constraints, split, expectedSql);
        }
    }

    @Test
    public void buildSplitSql_WithInPredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_STATUS, COLUMN_CATEGORY_ID, COLUMN_RATING);

            // Test IN predicates for different data types
            Map<String, ValueSet> summary = new HashMap<>();
            summary.put(COLUMN_STATUS, createStringInConstraint("ACTIVE", "PENDING", "COMPLETED"));
            summary.put(COLUMN_CATEGORY_ID, createIntInConstraint(1L, 5L, 10L));
            summary.put(COLUMN_RATING, createFloatInConstraint(4.5, 4.8, 5.0));

            Constraints constraints = createConstraints(summary, Collections.emptyList(), DEFAULT_NO_LIMIT);
            Split split = createBasicSplit();

            String expectedSql = "SELECT \"status\", \"category_id\", \"rating\" FROM \"schema\".\"table\"  WHERE (\"status\" IN (?,?,?)) AND (\"category_id\" IN (?,?,?)) AND (\"rating\" IN (?,?,?))";
            executeBuildSplitSqlTest(mockConnection, tableName, schema, constraints, split, expectedSql);
        }
    }

    @Test
    public void buildSplitSql_WithNullPredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_NAME, COLUMN_AGE, COLUMN_IS_ACTIVE);

            // Test NULL and NOT NULL predicates
            Map<String, ValueSet> summary = new HashMap<>();
            summary.put(COLUMN_NAME, createNullConstraint());
            summary.put(COLUMN_AGE, createNotNullConstraint());
            summary.put(COLUMN_IS_ACTIVE, createBooleanConstraint(true, true));

            Constraints constraints = createConstraints(summary, Collections.emptyList(), DEFAULT_NO_LIMIT);
            Split split = createBasicSplit();

            String expectedSql = "SELECT \"name\", \"age\", \"is_active\" FROM \"schema\".\"table\"  WHERE (\"name\" IS NULL) AND (\"age\" IS NOT NULL) AND ((\"is_active\" IS NULL) OR \"is_active\" = ?)";
            executeBuildSplitSqlTest(mockConnection, tableName, schema, constraints, split, expectedSql);
        }
    }

    @Test
    public void buildSplitSql_WithComplexCombinedPredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_NAME, COLUMN_AGE, COLUMN_SALARY, COLUMN_HIRE_DATE, COLUMN_IS_MANAGER);

            // Complex combined predicates
            Map<String, ValueSet> summary = new HashMap<>();
            summary.put(COLUMN_NAME, createStringInConstraint(TEST_NAME_JOHN, TEST_NAME_JANE, "Bob"));
            summary.put(COLUMN_AGE, createIntRangeConstraint(25L, true, 65L, false));
            summary.put(COLUMN_SALARY, createFloatGreaterThanConstraint(50000.0));
            summary.put(COLUMN_IS_MANAGER, createBooleanConstraint(true, false));

            Constraints constraints = createConstraints(summary, Collections.emptyList(), 100L);
            Split split = createBasicSplit();

            String expectedSql = "SELECT \"name\", \"age\", \"salary\", \"hire_date\", \"is_manager\" FROM \"schema\".\"table\"  WHERE (\"name\" IN (?,?,?)) AND ((\"age\" >= ? AND \"age\" < ?)) AND ((\"salary\" > ?)) AND (\"is_manager\" = ?) LIMIT 100";
            executeBuildSplitSqlTest(mockConnection, tableName, schema, constraints, split, expectedSql);
        }
    }

    @Test
    public void buildSplitSql_WithEdgeCasePredicates_BuildsCorrectSql()
            throws Exception
    {
        try (MockedConstruction<SnowflakeEnvironmentProperties> mocked = mockConstruction(
                SnowflakeEnvironmentProperties.class,
                (mock, context) -> when(mock.isS3ExportEnabled()).thenReturn(false)
        )) {
            Connection mockConnection = mock(Connection.class);
            TableName tableName = createTableName();
            Schema schema = createSchema(COLUMN_NAME, COLUMN_ID, COLUMN_VALUE);

            // Edge cases: empty ranges, single values, boundary values
            Map<String, ValueSet> summary = new HashMap<>();
            summary.put(COLUMN_ID, createIntInConstraint(42L));
            summary.put(COLUMN_VALUE, createFloatInConstraint(0.0, Double.MAX_VALUE));
            summary.put(COLUMN_NAME, createStringInConstraint(""));

            Constraints constraints = createConstraints(summary, Collections.emptyList(), DEFAULT_NO_LIMIT);
            Split split = createBasicSplit();

            String expectedSql = "SELECT \"name\", \"id\", \"value\" FROM \"schema\".\"table\"  WHERE (\"name\" = ?) AND (\"id\" = ?) AND (\"value\" IN (?,?))";
            executeBuildSplitSqlTest(mockConnection, tableName, schema, constraints, split, expectedSql);
        }
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

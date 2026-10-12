/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;

import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcRecordHandlerTest
        extends TestBase
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL2 = "testCol2";
    private static final String TEST_PARTITION_COL = "testPartitionCol";
    private static final String TEST_PARTITION_VALUE = "testPartitionValue";
    private static final String TEST_VAL1 = "testVal1";
    private static final String TEST_VAL2 = "testVal2";
    private static final String CONNECTION_STRING = "fakedatabase://jdbc:fakedatabase://hostname/${" + TEST_SECRET + "}";
    private static final String TEST_SQL = "someSql";

    private JdbcRecordHandler jdbcRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private QueryStatusChecker queryStatusChecker;
    private FederatedIdentity federatedIdentity;
    private PreparedStatement preparedStatement;

    @Before
    public void setup()
            throws Exception
    {
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        when(this.queryStatusChecker.isQueryRunning()).thenReturn(true);
        when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.preparedStatement = Mockito.mock(PreparedStatement.class);
        when(this.connection.prepareStatement(TEST_SQL)).thenReturn(this.preparedStatement);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, "fakedatabase",
                CONNECTION_STRING, TEST_SECRET);
        this.jdbcRecordHandler = new JdbcRecordHandler(this.amazonS3, this.secretsManager, this.athena, databaseConnectionConfig, this.jdbcConnectionFactory, com.google.common.collect.ImmutableMap.of())
        {
            @Override
            public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
                    throws SQLException
            {
                return jdbcConnection.prepareStatement(TEST_SQL);
            }
        };
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @Test
    public void close_delegatesToJdbcConnectionFactory()
            throws Exception
    {
        this.jdbcRecordHandler.close();
        Mockito.verify(this.jdbcConnectionFactory, Mockito.times(1)).close();
    }

    @Test
    public void close_withNullFactory_isNoOp()
            throws Exception
    {
        // Simulate the multiplexing-constructor case where the base handler has no factory of its own.
        // The null-factory branch of close() should be a no-op and MUST NOT throw.
        java.lang.reflect.Field factoryField = JdbcRecordHandler.class.getDeclaredField("jdbcConnectionFactory");
        factoryField.setAccessible(true);
        factoryField.set(this.jdbcRecordHandler, null);

        this.jdbcRecordHandler.close();
    }

    @Test
    public void readWithConstraint()
            throws Exception
    {
        ConstraintEvaluator constraintEvaluator = Mockito.mock(ConstraintEvaluator.class);
        when(constraintEvaluator.apply(nullable(String.class), any())).thenReturn(true);

        TableName inputTableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL2, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION_COL, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema fieldSchema = expectedSchemaBuilder.build();

        BlockAllocator allocator = new BlockAllocatorImpl();
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();

        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add(TEST_PARTITION_COL, TEST_PARTITION_VALUE);

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        String[] schema = {TEST_COL1, TEST_COL2};
        int[] columnTypes = {Types.INTEGER, Types.VARCHAR};
        Object[][] values = {{1, TEST_VAL1}, {2, TEST_VAL2}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, columnTypes, values, rowNumber);
        when(this.preparedStatement.executeQuery()).thenReturn(resultSet);

        // Mocking database metadata to return a non-ClickHouse database name eg:MySQL
        DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
        when(metaData.getDatabaseProductName()).thenReturn("MySQL");
        when(this.connection.getMetaData()).thenReturn(metaData);

        SpillConfig spillConfig = Mockito.mock(SpillConfig.class);
        when(spillConfig.getSpillLocation()).thenReturn(s3SpillLocation);
        BlockSpiller s3Spiller = new S3BlockSpiller(this.amazonS3, spillConfig, allocator, fieldSchema, constraintEvaluator, com.google.common.collect.ImmutableMap.of());
        ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID, inputTableName, fieldSchema, splitBuilder.build(), constraints, 1024, 1024);

        when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteArrayInputStream inputStream = (ByteArrayInputStream) ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    int n = inputStream.available();
                    byte[] bytes = new byte[n];
                    inputStream.read(bytes, 0, n);
                    String data = new String(bytes, StandardCharsets.UTF_8);
                    Assert.assertTrue(data.contains(TEST_VAL1) || data.contains(TEST_VAL2) || data.contains(TEST_PARTITION_VALUE));
                    return PutObjectResponse.builder().build();
                });

        this.jdbcRecordHandler.readWithConstraint(s3Spiller, readRecordsRequest, queryStatusChecker);
    }
    @Test
    public void makeExtractor()
            throws Exception
    {
        String[] schema = {TEST_COL1, TEST_COL2, "testCol10"};
        int[] columnTypes = {Types.INTEGER, Types.VARCHAR, Types.DOUBLE};
        Object[][] values = {{1, TEST_VAL1, "$1,000.50"}, {2, TEST_VAL2, "$100.00"}};
        AtomicInteger rowNumber = new AtomicInteger(0);

        ResultSet resultSet = mockResultSet(schema, columnTypes, values, rowNumber);
        when(this.preparedStatement.executeQuery()).thenReturn(resultSet);
        Map<String,String> partitionMap = Collections.singletonMap(TEST_PARTITION_COL, TEST_PARTITION_VALUE);

        Extractor actualInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder(TEST_COL1, org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build(),resultSet,partitionMap);
        Extractor actualVarchar = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder(TEST_COL2, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),resultSet,partitionMap);
        Extractor actualBit = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.BIT.getType()).build(),resultSet,partitionMap);
        Extractor actualTinyInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType()).build(),resultSet,partitionMap);
        Extractor actualSmallInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol5", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType()).build(),resultSet,partitionMap);
        Extractor actualVarbinary = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol6", org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType()).build(),resultSet,partitionMap);
        Extractor actualBigInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol8", org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType()).build(),resultSet,partitionMap);
        Extractor actualFloat4 = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol9", org.apache.arrow.vector.types.Types.MinorType.FLOAT4.getType()).build(),resultSet,partitionMap);
        Extractor actualFloat8 = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol10", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build(),resultSet,partitionMap);
        Extractor actualDateDay = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol11", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType()).build(),resultSet,partitionMap);
        Extractor actualDateMilli = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol12", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build(),resultSet,partitionMap);

        Assert.assertTrue(actualInt instanceof IntExtractor);
        Assert.assertTrue(actualVarchar instanceof VarCharExtractor);
        Assert.assertTrue(actualBit instanceof BitExtractor);
        Assert.assertTrue(actualTinyInt instanceof TinyIntExtractor);
        Assert.assertTrue(actualSmallInt instanceof SmallIntExtractor);
        Assert.assertTrue(actualVarbinary instanceof VarBinaryExtractor);
        Assert.assertTrue(actualBigInt instanceof BigIntExtractor);
        Assert.assertTrue(actualFloat4 instanceof Float4Extractor);
        Assert.assertTrue(actualFloat8 instanceof Float8Extractor);
        Assert.assertTrue(actualDateDay instanceof DateDayExtractor);
        Assert.assertTrue(actualDateMilli instanceof DateMilliExtractor);

        NullableFloat8Holder dollarValue = new NullableFloat8Holder();
        ((Float8Extractor) actualFloat8).extract(null, dollarValue);
        Assert.assertEquals(1000.5, dollarValue.value, 0.0);
    }

    @Test
    public void testMakeExtractorWithoutNullValues() throws Exception {
        Map<String, String> partitionMap = Collections.singletonMap("testPartitionCol", "testPartitionValue");
        byte[] bytes = "test".getBytes();
        Date date = Date.valueOf(LocalDate.of(2025, 4, 22));
        Timestamp time = Timestamp.valueOf(LocalDateTime.of(2025, 4, 22, 5, 30));

        ResultSet resultSet = Mockito.mock(ResultSet.class, Mockito.RETURNS_DEEP_STUBS);

        when(resultSet.getInt("testCol1")).thenReturn(10);
        when(resultSet.getString("testCol2")).thenReturn("test");
        when(resultSet.getString("varchar_col")).thenReturn("test value");  // Added for VARCHAR test
        when(resultSet.getBoolean("testCol3")).thenReturn(true);
        when(resultSet.getByte("testCol4")).thenReturn((byte) 100);
        when(resultSet.getShort("testCol5")).thenReturn((short) 1234);
        when(resultSet.getBytes("testCol6")).thenReturn(bytes);
        when(resultSet.getLong("testCol8")).thenReturn(10000L);
        when(resultSet.getFloat("testCol9")).thenReturn(123f);
        when(resultSet.getDate("testCol11")).thenReturn(date);
        when(resultSet.getTimestamp("testCol12")).thenReturn(time);

        when(resultSet.wasNull()).thenReturn(false);

        Extractor actualInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build(), resultSet, partitionMap);
        Extractor actualVarchar = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(), resultSet, partitionMap);
        Extractor actualVarcharTest = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("varchar_col", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(), resultSet, partitionMap);
        Extractor actualBit = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.BIT.getType()).build(), resultSet, partitionMap);
        Extractor actualTinyInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType()).build(), resultSet, partitionMap);
        Extractor actualSmallInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol5", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType()).build(), resultSet, partitionMap);
        Extractor actualVarbinary = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol6", org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType()).build(), resultSet, partitionMap);
        Extractor actualBigInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol8", org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType()).build(), resultSet, partitionMap);
        Extractor actualFloat4 = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol9", org.apache.arrow.vector.types.Types.MinorType.FLOAT4.getType()).build(), resultSet, partitionMap);
        Extractor actualDateDay = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol11", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType()).build(), resultSet, partitionMap);
        Extractor actualDateMilli = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol12", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build(), resultSet, partitionMap);

        // Test non-NULL values
        NullableIntHolder intHolder = new NullableIntHolder();
        ((IntExtractor) actualInt).extract(null, intHolder);
        Assert.assertEquals(10, intHolder.value);
        Assert.assertEquals(1, intHolder.isSet);

        NullableVarCharHolder varHolder = new NullableVarCharHolder();
        ((VarCharExtractor) actualVarchar).extract(null, varHolder);
        Assert.assertEquals("test", varHolder.value);
        Assert.assertEquals(1, varHolder.isSet);

        NullableVarCharHolder varTestHolder = new NullableVarCharHolder();
        ((VarCharExtractor) actualVarcharTest).extract(null, varTestHolder);
        Assert.assertEquals("test value", varTestHolder.value);
        Assert.assertEquals(1, varTestHolder.isSet);

        NullableBitHolder bitHolder = new NullableBitHolder();
        ((BitExtractor) actualBit).extract(null, bitHolder);
        Assert.assertEquals(1, bitHolder.value);
        Assert.assertEquals(1, bitHolder.isSet);

        NullableTinyIntHolder tinyIntHolder = new NullableTinyIntHolder();
        ((TinyIntExtractor) actualTinyInt).extract(null, tinyIntHolder);
        Assert.assertEquals(100, tinyIntHolder.value);
        Assert.assertEquals(1, tinyIntHolder.isSet);

        NullableSmallIntHolder smallIntHolder = new NullableSmallIntHolder();
        ((SmallIntExtractor) actualSmallInt).extract(null, smallIntHolder);
        Assert.assertEquals(1234, smallIntHolder.value);
        Assert.assertEquals(1, smallIntHolder.isSet);

        NullableVarBinaryHolder varBinaryHolder = new NullableVarBinaryHolder();
        ((VarBinaryExtractor) actualVarbinary).extract(null, varBinaryHolder);
        Assert.assertEquals(bytes, varBinaryHolder.value);
        Assert.assertEquals(1, varBinaryHolder.isSet);

        NullableBigIntHolder bigIntHolder = new NullableBigIntHolder();
        ((BigIntExtractor) actualBigInt).extract(null, bigIntHolder);
        Assert.assertEquals(10000L, bigIntHolder.value);
        Assert.assertEquals(1, bigIntHolder.isSet);

        NullableFloat4Holder float4Holder = new NullableFloat4Holder();
        ((Float4Extractor) actualFloat4).extract(null, float4Holder);
        Assert.assertEquals(123f, float4Holder.value, 0.0);
        Assert.assertEquals(1, float4Holder.isSet);

        NullableDateDayHolder dateDayHolder = new NullableDateDayHolder();
        ((DateDayExtractor) actualDateDay).extract(null, dateDayHolder);
        verify(resultSet, Mockito.times(2)).getDate(anyString());
        Assert.assertEquals(1, dateDayHolder.isSet);

        NullableDateMilliHolder dateMilliHolder = new NullableDateMilliHolder();
        ((DateMilliExtractor) actualDateMilli).extract(null, dateMilliHolder);
        verify(resultSet, Mockito.times(2)).getTimestamp(anyString());
        Assert.assertEquals(1, dateMilliHolder.isSet);
    }

    @Test
    public void testMakeExtractorWithNullValues() throws Exception {

        Map<String, String> partitionMap = Collections.singletonMap("testPartitionCol", "testPartitionValue");
        ResultSet resultSet = Mockito.mock(ResultSet.class, Mockito.RETURNS_DEEP_STUBS);

        when(resultSet.getInt("testCol1")).thenReturn(0); // Default value for NULL
        when(resultSet.getString("testCol2")).thenReturn(null);
        when(resultSet.getString("varchar_col")).thenReturn(null);
        when(resultSet.getBoolean("testCol3")).thenReturn(false); // Default value for NULL
        when(resultSet.getByte("testCol4")).thenReturn((byte) 0); // Default value for NULL
        when(resultSet.getShort("testCol5")).thenReturn((short) 0); // Default value for NULL
        when(resultSet.getBytes("testCol6")).thenReturn(null);
        when(resultSet.getLong("testCol8")).thenReturn(0L); // Default value for NULL
        when(resultSet.getFloat("testCol9")).thenReturn(0f); // Default value for NULL
        when(resultSet.getDouble("testCol10")).thenReturn(0.0); // Default value for NULL
        when(resultSet.getDate("testCol11")).thenReturn(null);
        when(resultSet.getTimestamp("testCol12")).thenReturn(null);

        when(resultSet.wasNull()).thenReturn(true);

        Extractor actualInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build(), resultSet, partitionMap);
        Extractor actualVarchar = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(), resultSet, partitionMap);
        Extractor actualVarcharTest = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("varchar_col", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(), resultSet, partitionMap);
        Extractor actualBit = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.BIT.getType()).build(), resultSet, partitionMap);
        Extractor actualTinyInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType()).build(), resultSet, partitionMap);
        Extractor actualSmallInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol5", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType()).build(), resultSet, partitionMap);
        Extractor actualVarbinary = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol6", org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType()).build(), resultSet, partitionMap);
        Extractor actualBigInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol8", org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType()).build(), resultSet, partitionMap);
        Extractor actualFloat4 = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol9", org.apache.arrow.vector.types.Types.MinorType.FLOAT4.getType()).build(), resultSet, partitionMap);
        Extractor actualFloat8 = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol10", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build(), resultSet, partitionMap);
        Extractor actualDateDay = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol11", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType()).build(), resultSet, partitionMap);
        Extractor actualDateMilli = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol12", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build(), resultSet, partitionMap);

        NullableIntHolder intHolder = new NullableIntHolder();
        ((IntExtractor) actualInt).extract(null, intHolder);
        Assert.assertEquals(0, intHolder.isSet);
        Assert.assertEquals(0, intHolder.value); // Default value for NULL

        NullableVarCharHolder varHolder = new NullableVarCharHolder();
        ((VarCharExtractor) actualVarchar).extract(null, varHolder);
        Assert.assertEquals(0, varHolder.isSet);
        Assert.assertNull(varHolder.value);

        NullableVarCharHolder varTestHolder = new NullableVarCharHolder();
        ((VarCharExtractor) actualVarcharTest).extract(null, varTestHolder);
        Assert.assertEquals(0, varTestHolder.isSet);
        Assert.assertNull(varTestHolder.value);

        NullableBitHolder bitHolder = new NullableBitHolder();
        ((BitExtractor) actualBit).extract(null, bitHolder);
        Assert.assertEquals(0, bitHolder.isSet);
        Assert.assertEquals(0, bitHolder.value); // Default value for NULL

        NullableTinyIntHolder tinyIntHolder = new NullableTinyIntHolder();
        ((TinyIntExtractor) actualTinyInt).extract(null, tinyIntHolder);
        Assert.assertEquals(0, tinyIntHolder.isSet);
        Assert.assertEquals(0, tinyIntHolder.value); // Default value for NULL

        NullableSmallIntHolder smallIntHolder = new NullableSmallIntHolder();
        ((SmallIntExtractor) actualSmallInt).extract(null, smallIntHolder);
        Assert.assertEquals(0, smallIntHolder.isSet);
        Assert.assertEquals(0, smallIntHolder.value); // Default value for NULL

        NullableVarBinaryHolder varBinaryHolder = new NullableVarBinaryHolder();
        ((VarBinaryExtractor) actualVarbinary).extract(null, varBinaryHolder);
        Assert.assertEquals(0, varBinaryHolder.isSet);
        Assert.assertNull(varBinaryHolder.value);

        NullableBigIntHolder bigIntHolder = new NullableBigIntHolder();
        ((BigIntExtractor) actualBigInt).extract(null, bigIntHolder);
        Assert.assertEquals(0, bigIntHolder.isSet);
        Assert.assertEquals(0L, bigIntHolder.value); // Default value for NULL

        NullableFloat4Holder float4Holder = new NullableFloat4Holder();
        ((Float4Extractor) actualFloat4).extract(null, float4Holder);
        Assert.assertEquals(0, float4Holder.isSet);
        Assert.assertEquals(0f, float4Holder.value, 0.0); // Default value for NULL

        NullableFloat8Holder float8Holder = new NullableFloat8Holder();
        ((Float8Extractor) actualFloat8).extract(null, float8Holder);
        Assert.assertEquals(0, float8Holder.isSet);
        Assert.assertEquals(0.0, float8Holder.value, 0.0); // Default value for NULL

        NullableDateDayHolder dateDayHolder = new NullableDateDayHolder();
        ((DateDayExtractor) actualDateDay).extract(null, dateDayHolder);
        Assert.assertEquals(0, dateDayHolder.isSet);
        Assert.assertEquals(0, dateDayHolder.value); // Default value for NULL

        NullableDateMilliHolder dateMilliHolder = new NullableDateMilliHolder();
        ((DateMilliExtractor) actualDateMilli).extract(null, dateMilliHolder);
        Assert.assertEquals(0, dateMilliHolder.isSet);
        Assert.assertEquals(0L, dateMilliHolder.value); // Default value for NULL
    }

    @Test
    public void testMakeExtractorWithUnsupportedType()  {
        Map<String, String> partitionMap = Collections.singletonMap("testPartitionCol", "testPartitionValue");
        ResultSet resultSet = Mockito.mock(ResultSet.class, Mockito.RETURNS_DEEP_STUBS);

        Field unsupportedField = FieldBuilder.newBuilder("testColUnsupported", org.apache.arrow.vector.types.Types.MinorType.LIST.getType()).build();

        try {
            this.jdbcRecordHandler.makeExtractor(unsupportedField, resultSet, partitionMap);
            Assert.fail("Expected AthenaConnectorException for unsupported type LIST");
        } catch (AthenaConnectorException e) {
            Assert.assertEquals("Unhandled type LIST", e.getMessage());
            Assert.assertEquals(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString(), e.getErrorDetails().errorCode());
        }
    }

    @Test
    public void testBuildQueryPassthroughSql() throws Exception {
        String testQuery = "SELECT * FROM test_table";
        Map<String, String> queryArgs = new HashMap<>();
        queryArgs.put("QUERY", testQuery);
        queryArgs.put("schemaFunctionName", "SYSTEM.QUERY");

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 
                Constraints.DEFAULT_NO_LIMIT, queryArgs, null);

        PreparedStatement mockPreparedStmt = Mockito.mock(PreparedStatement.class);
        when(this.connection.prepareStatement(testQuery)).thenReturn(mockPreparedStmt);

        PreparedStatement result = this.jdbcRecordHandler.buildQueryPassthroughSql(this.connection, constraints);
        Assert.assertSame(mockPreparedStmt, result);
        verify(this.connection).prepareStatement(testQuery);
    }

    @Test
    public void testBuildQueryPassthroughSqlWithException() throws Exception {
        String testQuery = "SELECT * FROM test_table";
        Map<String, String> queryArgs = new HashMap<>();
        queryArgs.put("QUERY", testQuery);

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 
                Constraints.DEFAULT_NO_LIMIT, queryArgs, null);

        try {
            this.jdbcRecordHandler.buildQueryPassthroughSql(this.connection, constraints);
            Assert.fail("Expected AthenaConnectorException");
        } catch (AthenaConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Function Signature doesn't match implementation's"));
        }
    }

    @Test
    public void testMakeExtractorFloat8WithSQLException() throws Exception {
        Map<String, String> partitionValues = new HashMap<>();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        Field field = FieldBuilder.newBuilder(TEST_COL1, org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build();
        Float8Extractor extractor = (Float8Extractor) jdbcRecordHandler.makeExtractor(field, resultSet, partitionValues);
        
        NullableFloat8Holder nullableFloat8Holder = new NullableFloat8Holder();
        
        // Mock SQLException to trigger the parsing fallback
        Mockito.when(resultSet.getDouble(TEST_COL1)).thenThrow(new SQLException("Test SQLException"));
        Mockito.when(resultSet.getString(TEST_COL1)).thenReturn("$25,000.50");
        Mockito.when(resultSet.wasNull()).thenReturn(false);
        
        extractor.extract(resultSet, nullableFloat8Holder);
        
        Assert.assertEquals(1, nullableFloat8Holder.isSet);
        Assert.assertEquals(25000.50, nullableFloat8Holder.value, 0.01);
    }

    @Test
    public void testMakeExtractorWithPartitionValues() throws Exception {
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put(TEST_PARTITION_COL, TEST_PARTITION_VALUE);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        
        Field field = FieldBuilder.newBuilder(TEST_PARTITION_COL, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build();
        VarCharExtractor extractor = (VarCharExtractor) jdbcRecordHandler.makeExtractor(field, resultSet, partitionValues);
        
        NullableVarCharHolder nullableVarCharHolder = new NullableVarCharHolder();
        extractor.extract(resultSet, nullableVarCharHolder);
        
        Assert.assertEquals(1, nullableVarCharHolder.isSet);
        Assert.assertEquals(TEST_PARTITION_VALUE, nullableVarCharHolder.value);
    }

    @Test
    public void testEnableCaseSensitivelyLookUpSession() {
        Connection mockConnection = Mockito.mock(Connection.class);
        boolean result = jdbcRecordHandler.enableCaseSensitivelyLookUpSession(mockConnection);
        Assert.assertFalse(result); // Default implementation returns false
    }

    @Test
    public void testDisableCaseSensitivelyLookUpSession() {
        Connection mockConnection = Mockito.mock(Connection.class);
        boolean result = jdbcRecordHandler.disableCaseSensitivelyLookUpSession(mockConnection);
        Assert.assertFalse(result); // Default implementation returns false
    }

    @Test
    public void readWithConstraint_problemRowLoggingDisabled_omitsBadValueFromError()
            throws Exception
    {
        ResultSet resultSet = failingTimestampResultSet();
        Exception thrown = readFailingTimestampRow(this.jdbcRecordHandler, resultSet);

        Assert.assertFalse(thrown.getMessage().contains("created_at"));
        Assert.assertFalse(thrown.getMessage().contains("2018-00-15 04:59:59"));
        Mockito.verify(resultSet, Mockito.never()).getString(anyString());
    }

    @Test
    public void readWithConstraint_problemRowLoggingEnabled_capturesColumnValueAndPrimaryKey()
            throws Exception
    {
        ResultSet resultSet = failingTimestampResultSet();
        when(resultSet.getString("created_at")).thenReturn("2018-00-15 04:59:59");
        when(resultSet.getString("id")).thenReturn("2");
        stubPrimaryKey("id");

        CapturedProblemRow captured = new CapturedProblemRow();
        Exception thrown = readFailingTimestampRow(handlerWithProblemRowLogging(captured), resultSet);

        Assert.assertEquals("created_at", captured.columnName);
        Assert.assertEquals("2018-00-15 04:59:59", captured.value);
        Assert.assertEquals("id=2", captured.primaryKey);
        Assert.assertFalse(thrown.getMessage().contains("2018-00-15 04:59:59"));
    }

    @Test
    public void readWithConstraint_problemRowLoggingEnabled_truncatesLongColumnValue()
            throws Exception
    {
        String longValue = "2018-00-15 04:59:59 " + "x".repeat(300);
        ResultSet resultSet = failingTimestampResultSet();
        when(resultSet.getString("created_at")).thenReturn(longValue);
        when(resultSet.getRow()).thenReturn(12);

        CapturedProblemRow captured = new CapturedProblemRow();
        Exception thrown = readFailingTimestampRow(handlerWithProblemRowLogging(captured), resultSet);

        Assert.assertEquals("created_at", captured.columnName);
        Assert.assertEquals(longValue.substring(0, 256) + "...", captured.value);
        Assert.assertEquals("unavailable(row=12)", captured.primaryKey);
        Assert.assertFalse(thrown.getMessage().contains(longValue));
    }

    @Test
    public void readWithConstraint_primaryKeyNotProjectedAndGetRowFails_usesCountedRowNumber()
            throws Exception
    {
        ResultSet resultSet = failingTimestampResultSet();
        when(resultSet.getString("created_at")).thenReturn("2018-00-15 04:59:59");
        when(resultSet.getRow()).thenThrow(new SQLException("Operation not allowed for a result set of type ResultSet.TYPE_FORWARD_ONLY."));
        when(this.connection.getMetaData().getDatabaseProductName()).thenReturn("MySQL");
        stubPrimaryKey("unprojected_id");

        CapturedProblemRow captured = new CapturedProblemRow();
        readFailingTimestampRow(handlerWithProblemRowLogging(captured), resultSet);

        Assert.assertEquals("unprojected_id=not-in-projection(row=1)", captured.primaryKey);
        Mockito.verify(resultSet, Mockito.never()).getString("unprojected_id");
    }

    @Test
    public void lookupPrimaryKeyColumns_metadataFails_returnsEmptyList()
            throws Exception
    {
        when(this.connection.getCatalog()).thenReturn(TEST_CATALOG);
        when(this.connection.getMetaData().getPrimaryKeys(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE))
                .thenThrow(new SQLException("pk metadata unavailable"));

        List<String> keys = handlerWithProblemRowLogging(new CapturedProblemRow())
                .lookupPrimaryKeyColumns(this.connection, new TableName(TEST_SCHEMA, TEST_TABLE));

        Assert.assertTrue(keys.isEmpty());
    }

    @Test
    public void readWithConstraint_nullCellAndRowNumberUnavailable_logsNullValueAndUnreadableRow()
            throws Exception
    {
        ResultSet resultSet = failingTimestampResultSet();
        when(resultSet.getString("created_at")).thenReturn(null);
        when(resultSet.getRow()).thenReturn(0);

        CapturedProblemRow captured = new CapturedProblemRow();
        readFailingTimestampRow(handlerWithProblemRowLogging(captured), resultSet);

        Assert.assertEquals("created_at", captured.columnName);
        Assert.assertEquals("null", captured.value);
        Assert.assertEquals("unavailable(row=<unreadable>)", captured.primaryKey);
    }

    @Test
    public void readWithConstraint_problemRowLoggingEnabled_reportsFailuresForAllScalarTypes()
            throws Exception
    {
        // DATEMILLI + INT are already covered above; exercise remaining wrapExtractor cases once each.
        assertFailingColumnLogged("bit_col", org.apache.arrow.vector.types.Types.MinorType.BIT.getType(),
                rs -> when(rs.getBoolean("bit_col")).thenThrow(new SQLException("bad bit")));
        assertFailingColumnLogged("tiny_col", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType(),
                rs -> when(rs.getByte("tiny_col")).thenThrow(new SQLException("bad tiny")));
        assertFailingColumnLogged("small_col", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType(),
                rs -> when(rs.getShort("small_col")).thenThrow(new SQLException("bad small")));
        assertFailingColumnLogged("big_col", org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType(),
                rs -> when(rs.getLong("big_col")).thenThrow(new SQLException("bad big")));
        assertFailingColumnLogged("float4_col", org.apache.arrow.vector.types.Types.MinorType.FLOAT4.getType(),
                rs -> when(rs.getFloat("float4_col")).thenThrow(new SQLException("bad float4")));
        assertFailingColumnLogged("float8_col", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType(),
                rs -> when(rs.getDouble("float8_col")).thenThrow(new SQLException("bad float8")));
        assertFailingColumnLogged("decimal_col", new ArrowType.Decimal(10, 2, 128),
                rs -> when(rs.getBigDecimal("decimal_col")).thenThrow(new SQLException("bad decimal")));
        assertFailingColumnLogged("dateday_col", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType(),
                rs -> when(rs.getDate("dateday_col")).thenThrow(new SQLException("bad date")));
        assertFailingColumnLogged("varchar_col", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(),
                rs -> when(rs.getString("varchar_col")).thenThrow(new SQLException("bad varchar")));
        assertFailingColumnLogged("varbinary_col", org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType(),
                rs -> when(rs.getBytes("varbinary_col")).thenThrow(new SQLException("bad bytes")));
    }

    @Test
    public void readWithConstraint_arrayColumnFails_logsProblemRowForListField()
            throws Exception
    {
        Field listField = FieldBuilder.newBuilder("arr_col", org.apache.arrow.vector.types.Types.MinorType.LIST.getType())
                .addField(FieldBuilder.newBuilder("element", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build())
                .build();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.findColumn(anyString())).thenReturn(1);
        when(resultSet.getRow()).thenReturn(1);
        when(resultSet.getArray("arr_col")).thenThrow(new SQLException("bad array"));
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnLabel(1)).thenReturn("arr_col");
        when(metadata.getColumnName(1)).thenReturn("arr_col");
        when(resultSet.getMetaData()).thenReturn(metadata);

        CapturedProblemRow captured = new CapturedProblemRow();
        readFailingRow(handlerWithProblemRowLogging(captured), resultSet, SchemaBuilder.newBuilder().addField(listField).build());

        Assert.assertEquals("arr_col", captured.columnName);
        Assert.assertEquals("unavailable(row=1)", captured.primaryKey);
    }

    @Test
    public void readWithConstraint_problemRowLoggerThrows_preservesOriginalException()
            throws Exception
    {
        ResultSet resultSet = failingTimestampResultSet();
        when(resultSet.getString("created_at")).thenReturn("bad-ts");
        when(resultSet.getRow()).thenReturn(1);

        Map<String, String> config = Collections.singletonMap(JdbcRecordHandler.ENABLE_PROBLEM_ROW_LOGGING, "true");
        JdbcRecordHandler handler = new JdbcRecordHandler(this.amazonS3, this.secretsManager, this.athena,
                new DatabaseConnectionConfig(TEST_CATALOG, "fakedatabase", CONNECTION_STRING, TEST_SECRET),
                this.jdbcConnectionFactory, config)
        {
            @Override
            public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
                    throws SQLException
            {
                return jdbcConnection.prepareStatement(TEST_SQL);
            }

            @Override
            protected void reportProblemRow(String columnName, String stringValue, String primaryKey, Exception error)
            {
                throw new RuntimeException("logging failed");
            }
        };

        Exception thrown = readFailingTimestampRow(handler, resultSet);
        Assert.assertFalse(thrown.getMessage().contains("logging failed"));
    }

    @Test
    public void readWithConstraint_columnMetadataUnavailable_treatsPrimaryKeyAsUnprojected()
            throws Exception
    {
        ResultSet resultSet = failingTimestampResultSet();
        when(resultSet.getString("created_at")).thenReturn("bad-ts");
        when(resultSet.getMetaData()).thenThrow(new SQLException("metadata failed"));
        stubPrimaryKey("id");

        CapturedProblemRow captured = new CapturedProblemRow();
        readFailingTimestampRow(handlerWithProblemRowLogging(captured), resultSet);

        Assert.assertTrue(captured.primaryKey.startsWith("id=not-in-projection"));
    }

    private void assertFailingColumnLogged(String columnName, ArrowType arrowType, ResultSetStubber stubber)
            throws Exception
    {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.findColumn(anyString())).thenReturn(1);
        when(resultSet.getRow()).thenReturn(1);
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnLabel(1)).thenReturn(columnName);
        when(metadata.getColumnName(1)).thenReturn(columnName);
        when(resultSet.getMetaData()).thenReturn(metadata);
        stubber.stub(resultSet);

        CapturedProblemRow captured = new CapturedProblemRow();
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(columnName, arrowType).build())
                .build();
        readFailingRow(handlerWithProblemRowLogging(captured), resultSet, schema);

        Assert.assertEquals(columnName, captured.columnName);
        Assert.assertEquals("unavailable(row=1)", captured.primaryKey);
    }

    @FunctionalInterface
    private interface ResultSetStubber
    {
        void stub(ResultSet resultSet) throws Exception;
    }

    private JdbcRecordHandler handlerWithProblemRowLogging(CapturedProblemRow captured)
    {
        Map<String, String> config = Collections.singletonMap(JdbcRecordHandler.ENABLE_PROBLEM_ROW_LOGGING, "true");
        return new JdbcRecordHandler(this.amazonS3, this.secretsManager, this.athena,
                new DatabaseConnectionConfig(TEST_CATALOG, "fakedatabase", CONNECTION_STRING, TEST_SECRET),
                this.jdbcConnectionFactory, config)
        {
            
            @Override
            public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
                    throws SQLException
            {
                return jdbcConnection.prepareStatement(TEST_SQL);
            }
            @Override
            protected void reportProblemRow(String columnName, String stringValue, String primaryKey, Exception error)
            {
                captured.columnName = columnName;
                captured.value = stringValue;
                captured.primaryKey = primaryKey;
                // Also hit JdbcRecordHandler.reportProblemRow (LOGGER.warn path).
                super.reportProblemRow(columnName, stringValue, primaryKey, error);
            }
        };
    }

    private void stubPrimaryKey(String columnName)
            throws SQLException
    {
        ResultSet primaryKeys = Mockito.mock(ResultSet.class);
        when(primaryKeys.next()).thenReturn(true, false);
        when(primaryKeys.getShort("KEY_SEQ")).thenReturn((short) 1);
        when(primaryKeys.getString("COLUMN_NAME")).thenReturn(columnName);
        when(this.connection.getCatalog()).thenReturn(TEST_CATALOG);
        when(this.connection.getMetaData().getPrimaryKeys(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE)).thenReturn(primaryKeys);
    }

    private ResultSet failingTimestampResultSet()
            throws SQLException
    {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.findColumn(anyString())).thenReturn(1);
        when(resultSet.getInt("id")).thenReturn(2);
        when(resultSet.wasNull()).thenReturn(false);
        when(resultSet.getTimestamp("created_at")).thenThrow(new SQLException("MONTH"));
        ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnLabel(1)).thenReturn("id");
        when(metadata.getColumnLabel(2)).thenReturn("created_at");
        when(metadata.getColumnName(1)).thenReturn("id");
        when(metadata.getColumnName(2)).thenReturn("created_at");
        when(resultSet.getMetaData()).thenReturn(metadata);
        return resultSet;
    }

    private static final class CapturedProblemRow
    {
        private String columnName;
        private String value;
        private String primaryKey;
    }

    private Exception readFailingTimestampRow(JdbcRecordHandler handler, ResultSet resultSet)
            throws Exception
    {
        Schema fieldSchema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("id", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("created_at", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build())
                .build();
        return readFailingRow(handler, resultSet, fieldSchema);
    }

    private Exception readFailingRow(JdbcRecordHandler handler, ResultSet resultSet, Schema fieldSchema)
            throws Exception
    {
        when(this.preparedStatement.executeQuery()).thenReturn(resultSet);

        ConstraintEvaluator constraintEvaluator = Mockito.mock(ConstraintEvaluator.class);
        when(constraintEvaluator.apply(nullable(String.class), any())).thenReturn(true);
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
        SpillConfig spillConfig = Mockito.mock(SpillConfig.class);
        when(spillConfig.getSpillLocation()).thenReturn(s3SpillLocation);
        BlockAllocator allocator = new BlockAllocatorImpl();
        BlockSpiller spiller = new S3BlockSpiller(this.amazonS3, spillConfig, allocator, fieldSchema, constraintEvaluator, com.google.common.collect.ImmutableMap.of());

        Split split = Split.newBuilder(s3SpillLocation, null).build();
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        ReadRecordsRequest request = new ReadRecordsRequest(this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID,
                new TableName(TEST_SCHEMA, TEST_TABLE), fieldSchema, split, constraints, 1024, 1024);

        try {
            handler.readWithConstraint(spiller, request, this.queryStatusChecker);
            Assert.fail("Expected conversion of the incompatible row to fail");
            return null;
        }
        catch (Exception ex) {
            return ex;
        }
        finally {
            allocator.close();
        }
    }
}

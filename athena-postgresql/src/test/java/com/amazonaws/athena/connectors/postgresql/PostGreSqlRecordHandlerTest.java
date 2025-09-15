/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;

public class PostGreSqlRecordHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(PostGreSqlRecordHandlerTest.class);

    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_CATALOG = "testCatalogName";
    private static final String PARTITION_SCHEMA = "s0";
    private static final String PARTITION_NAME = "p0";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL2 = "testCol2";
    private static final String TEST_COL3 = "testCol3";
    private static final String TEST_COL4 = "testCol4";
    private static final String TEST_COL5 = "testCol5";
    private static final String TEST_COL6 = "testCol6";
    private static final String TEST_COL7 = "testCol7";
    private static final String TEST_COL8 = "testCol8";
    private static final String TEST_COL9 = "testCol9";
    private static final String TEST_COL10 = "testCol10";
    private static final String COL="col1";
    private static final String PARTITION_SCHEMA_NAME = "partition_schema_name";
    private static final String PARTITION_NAME_COL = "partition_name";
    private static final String COL_ID = "id";
    private static final String COL_NAME = "name";
    private static final String COL_CATEGORY = "category";
    private static final String COL_PRICE = "price";
    private static final String COL_RATING = "rating";
    private static final String COL_ACTIVE = "active";
    private static final String COL_INT = "intCol";
    private static final String COL_STRING = "stringCol";
    private static final String COL_FLOAT = "floatCol";
    private static final String COL_BOOL = "boolCol";
    private static final String COL_NULLABLE = "nullableCol";
    private static final String COL_REQUIRED = "requiredCol";
    private static final String COL_DATE = "dateCol";
    private static final String COL_SCORE = "scoreCol";
    private static final long LIMIT_5 = 5L;
    private static final long LIMIT_20 = 20L;
    private static final long LIMIT_25 = 25L;
    private static final long LIMIT_100 = 100L;
    private static final String STATUS_ACTIVE = "ACTIVE";
    private static final String STATUS_PENDING = "PENDING";
    private PostGreSqlRecordHandler postGreSqlRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private MockedStatic<PostGreSqlMetadataHandler> mockedPostGreSqlMetadataHandler;

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
        jdbcSplitQueryBuilder = new PostGreSqlQueryStringBuilder("\"", new PostgreSqlFederationExpressionParser("\""));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", POSTGRES_NAME,
                "postgres://jdbc:postgresql://hostname/user=A&password=B");

        this.postGreSqlRecordHandler = new PostGreSqlRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
        mockedPostGreSqlMetadataHandler = Mockito.mockStatic(PostGreSqlMetadataHandler.class);
        mockedPostGreSqlMetadataHandler.when(() -> PostGreSqlMetadataHandler.getCharColumns(any(), anyString(), anyString())).thenReturn(Collections.singletonList("testCol10"));
    }

    @After
    public void close(){
        mockedPostGreSqlMetadataHandler.close();
    }

    @Test
    public void buildSplitSqlTest()
            throws SQLException
    {
        logger.info("buildSplitSqlTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL3, Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL5, Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL6, Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL7, Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL8, Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL9, new ArrowType.Decimal(8, 2,128)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL10, new ArrowType.Utf8()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit("s0", "p0");

        // Create value sets for all test columns
        ValueSet valueSet1 = createMultiValueSet(Arrays.asList(1, 2));
        ValueSet valueSet2 = getRangeSet(Marker.Bound.EXACTLY, "1", Marker.Bound.BELOW, "10");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.ABOVE, 2L, Marker.Bound.EXACTLY, 20L);
        ValueSet valueSet4 = getSingleValueSet(1.1F);
        ValueSet valueSet5 = getSingleValueSet(1);
        ValueSet valueSet6 = getSingleValueSet(0);
        ValueSet valueSet7 = getSingleValueSet(1.2d);
        ValueSet valueSet8 = getSingleValueSet(true);
        ValueSet valueSet9 = getSingleValueSet(BigDecimal.valueOf(12.34));
        ValueSet valueSet10 = getSingleValueSet("A");

        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put(TEST_COL1, valueSet1)
                .put(TEST_COL2, valueSet2)
                .put(TEST_COL3, valueSet3)
                .put(TEST_COL4, valueSet4)
                .put(TEST_COL5, valueSet5)
                .put(TEST_COL6, valueSet6)
                .put(TEST_COL7, valueSet7)
                .put(TEST_COL8, valueSet8)
                .put(TEST_COL9, valueSet9)
                .put(TEST_COL10, valueSet10)
                .build());

        String expectedSql = String.format("SELECT \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", RTRIM(\"%s\") AS \"%s\" FROM \"%s\".\"%s\"  WHERE (\"%s\" IN (?,?)) AND ((\"%s\" >= ? AND \"%s\" < ?)) AND ((\"%s\" > ? AND \"%s\" <= ?)) AND (\"%s\" = ?) AND (\"%s\" = ?) AND (\"%s\" = ?) AND (\"%s\" = ?) AND (\"%s\" = ?) AND (\"%s\" = ?) AND (\"%s\" = ?)",
                TEST_COL1, TEST_COL2, TEST_COL3, TEST_COL4, TEST_COL5, TEST_COL6, TEST_COL7, TEST_COL8, TEST_COL9, TEST_COL10, TEST_COL10,
                PARTITION_SCHEMA, PARTITION_NAME,
                TEST_COL1, TEST_COL2, TEST_COL2, TEST_COL3, TEST_COL3, TEST_COL4, TEST_COL5, TEST_COL6, TEST_COL7, TEST_COL8, TEST_COL9, TEST_COL10);
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "1");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, "10");
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(5, 2L);
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(6, 20L);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(7, 1.1F);
        Mockito.verify(preparedStatement, Mockito.times(1)).setShort(8, (short) 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setByte(9, (byte) 0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(10, 1.2d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBoolean(11, true);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(12, BigDecimal.valueOf(12.34));
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(13, "A");

        logger.info("buildSplitSqlTest - exit");
    }

    @Test
    public void buildSplitSqlForDateTest()
            throws SQLException
    {
        logger.info("buildSplitSqlForDateTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testDate", Types.MinorType.DATEDAY.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit("s0", PARTITION_NAME);

        final long dateDays = TimeUnit.MILLISECONDS.toDays(Date.valueOf("2020-01-05").getTime());
        Constraints constraints = createConstraints("testDate", getSingleValueSet(dateDays));

        String expectedSql = "SELECT \"testDate\" FROM \"s0\".\"p0\"  WHERE (\"testDate\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        //From sql.Date java doc. Params:
        //year – the year minus 1900; must be 0 to 8099. (Note that 8099 is 9999 minus 1900.)
        //month – 0 to 11
        //day – 1 to 31
        //Start date = 1992-1-1
        Date expectedDate = Date.valueOf("2020-01-05");
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1))
                .setDate(1, expectedDate);

        logger.info("buildSplitSqlForDateTest - exit");
    }

    private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test(expected = AthenaConnectorException.class)
    public void buildSplitSqlWithConnectionErrorTest()
            throws SQLException
    {
        logger.info("buildSplitSqlWithConnectionErrorTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);
        Constraints constraints = createConstraints(COL, getSingleValueSet(1));

        // Simulate connection error
        createMockPreparedStatementWithError("Connection refused");

        // This should throw SQLException
        this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
    }

    @Test(expected = AthenaConnectorException.class)
    public void buildSplitSqlWithSyntaxErrorTest()
            throws SQLException
    {
        logger.info("buildSplitSqlWithSyntaxErrorTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        // Create an invalid value that will cause SQL syntax error
        Constraints constraints = createConstraints("col1", getSingleValueSet("invalid'value"));

        // Simulate syntax error
        createMockPreparedStatementWithError("Syntax error in SQL statement");

        // This should throw SQLException
        this.postGreSqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        logger.info("buildSplitSqlWithSyntaxErrorTest - exit");
    }

    @Test(expected = AthenaConnectorException.class)
    public void buildSplitSqlWithPermissionErrorTest()
            throws SQLException
    {
        logger.info("buildSplitSqlWithPermissionErrorTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);
        Constraints constraints = createConstraints(COL, getSingleValueSet(1));

        // Simulate permission error
        createMockPreparedStatementWithError("permission denied for table testTable");

        // This should throw SQLException
        this.postGreSqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        logger.info("buildSplitSqlWithPermissionErrorTest - exit");
    }

    @Test
    public void buildSplitSqlWithEmptyResultTest()
            throws SQLException
    {
        logger.info("buildSplitSqlWithEmptyResultTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("col2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_SCHEMA_NAME, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NAME_COL, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of(PARTITION_SCHEMA_NAME, PARTITION_SCHEMA, PARTITION_NAME_COL, PARTITION_NAME));
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn(PARTITION_SCHEMA);
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn(PARTITION_NAME);

        // Create constraints that will result in no matches
        Range emptyRange = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(emptyRange.isSingleValue()).thenReturn(false);
        Mockito.when(emptyRange.getLow().getBound()).thenReturn(Marker.Bound.ABOVE);
        Mockito.when(emptyRange.getLow().getValue()).thenReturn(10);
        Mockito.when(emptyRange.getHigh().getBound()).thenReturn(Marker.Bound.BELOW);
        Mockito.when(emptyRange.getHigh().getValue()).thenReturn(5);
        ValueSet emptyValueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(emptyValueSet.isNone()).thenReturn(false);
        Mockito.when(emptyValueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(emptyRange));

        ValueSet valueSet2 = getSingleValueSet("test");

        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put("col1", emptyValueSet)  // Empty range: col1 > 10 AND col1 < 5 (impossible)
                .put("col2", valueSet2)
                .build());

        String expectedSql = "SELECT \"col1\", \"col2\" FROM \"s0\".\"p0\"  WHERE ((\"col1\" > ? AND \"col1\" < ?)) AND (\"col2\" = ?)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameters were set correctly
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 10);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 5);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "test");

        logger.info("buildSplitSqlWithEmptyResultTest - exit");
    }

    @Test
    public void buildSplitSqlWithLargeNumbersTest()
            throws SQLException
    {
        logger.info("buildSplitSqlWithLargeNumbersTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        // Add numeric fields with different precisions
        schemaBuilder.addField(FieldBuilder.newBuilder("bigintCol", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("decimal38_10", new ArrowType.Decimal(38, 10, 128)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("decimal18_6", new ArrowType.Decimal(18, 6, 128)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("decimal9_3", new ArrowType.Decimal(9, 3, 128)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_schema_name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of(PARTITION_SCHEMA_NAME, PARTITION_SCHEMA, PARTITION_NAME_COL, PARTITION_NAME));
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn(PARTITION_SCHEMA);
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn(PARTITION_NAME);

        // Create test values with large numbers and high precision
        long bigintValue = Long.MAX_VALUE; // 9,223,372,036,854,775,807
        BigDecimal decimal38_10Value = new BigDecimal("12345678901234567890.1234567890"); // 28 digits before decimal, 10 after
        BigDecimal decimal18_6Value = new BigDecimal("123456789012.123456"); // 12 digits before decimal, 6 after
        BigDecimal decimal9_3Value = new BigDecimal("123456.789"); // 6 digits before decimal, 3 after

        // Create value sets for numeric columns
        ValueSet bigintValueSet = getSingleValueSet(bigintValue);
        ValueSet decimal38_10ValueSet = getSingleValueSet(decimal38_10Value);
        ValueSet decimal18_6ValueSet = getSingleValueSet(decimal18_6Value);
        ValueSet decimal9_3ValueSet = getSingleValueSet(decimal9_3Value);

        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put("bigintCol", bigintValueSet)
                .put("decimal38_10", decimal38_10ValueSet)
                .put("decimal18_6", decimal18_6ValueSet)
                .put("decimal9_3", decimal9_3ValueSet)
                .build());

        String expectedSql = "SELECT \"bigintCol\", \"decimal38_10\", \"decimal18_6\", \"decimal9_3\" FROM \"s0\".\"p0\"  WHERE (\"bigintCol\" = ?) AND (\"decimal38_10\" = ?) AND (\"decimal18_6\" = ?) AND (\"decimal9_3\" = ?)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify numeric parameters were set correctly
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(1, bigintValue);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(2, decimal38_10Value);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(3, decimal18_6Value);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(4, decimal9_3Value);

        logger.info("buildSplitSqlWithLargeNumbersTest - exit");
    }

    @Test
    public void buildSplitSqlWithSpecialCharsTest()
            throws SQLException
    {
        logger.info("buildSplitSqlWithSpecialCharsTest - enter");

        TableName tableName = new TableName("test Schema", "test Table");

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        // Add fields with special characters in names
        schemaBuilder.addField(FieldBuilder.newBuilder("column with spaces", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("column\"with\"quotes", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("column'with'apostrophes", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("column;with;semicolons", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        // Create test values with special characters
        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put("column with spaces", getSingleValueSet("value with spaces"))
                .put("column\"with\"quotes", getSingleValueSet("value\"with\"quotes"))
                .put("column'with'apostrophes", getSingleValueSet("value'with'apostrophes"))
                .put("column;with;semicolons", getSingleValueSet("value;with;semicolons"))
                .build());

        String expectedSql = "SELECT \"column with spaces\", \"column\"\"with\"\"quotes\", \"column'with'apostrophes\", \"column;with;semicolons\" FROM \"s0\".\"p0\"  WHERE (\"column with spaces\" = ?) AND (\"column\"\"with\"\"quotes\" = ?) AND (\"column'with'apostrophes\" = ?) AND (\"column;with;semicolons\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameters were set correctly
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "value with spaces");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, "value\"with\"quotes");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "value'with'apostrophes");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, "value;with;semicolons");

        logger.info("buildSplitSqlWithSpecialCharsTest - exit");
    }

    @Test
    public void buildSplitSqlWithJsonTest()
            throws SQLException
    {
        logger.info("buildSplitSqlWithJsonTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        // Add JSON and JSONB fields using VARCHAR type since Athena represents them as strings
        schemaBuilder.addField(FieldBuilder.newBuilder("jsonCol", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("jsonbCol", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        // Create JSON test values
        String jsonValue = "{\"key\": \"value\", \"number\": 42}";
        String jsonbValue = "{\"array\": [1, 2, 3], \"nested\": {\"field\": \"test\"}}";

        // Create value sets for JSON columns
        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put("jsonCol", getSingleValueSet(jsonValue))
                .put("jsonbCol", getSingleValueSet(jsonbValue))
                .build());

        String expectedSql = "SELECT \"jsonCol\", \"jsonbCol\" FROM \"s0\".\"p0\"  WHERE (\"jsonCol\" = ?) AND (\"jsonbCol\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify JSON parameters were set correctly
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, jsonValue);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, jsonbValue);

        logger.info("buildSplitSqlWithJsonTest - exit");
    }

    @Test
    public void buildSplitSqlWithArrayTest()
            throws SQLException
    {
        logger.info("buildSplitSqlWithArrayTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        // Add array fields using VARCHAR type since arrays are handled as strings in the connector
        schemaBuilder.addField(FieldBuilder.newBuilder("intArrayCol", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("stringArrayCol", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        // Create array values for testing
        String intArrayStr = "{1,2,3}";
        String stringArrayStr = "{\"a\",\"b\",\"c\"}";

        // Create value sets for array columns
        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put("intArrayCol", getSingleValueSet(intArrayStr))
                .put("stringArrayCol", getSingleValueSet(stringArrayStr))
                .build());

        String expectedSql = "SELECT \"intArrayCol\", \"stringArrayCol\" FROM \"s0\".\"p0\"  WHERE (\"intArrayCol\" = ?) AND (\"stringArrayCol\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify array parameters were set correctly
        // Arrays are handled as strings in the connector
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, intArrayStr);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, stringArrayStr);

        logger.info("buildSplitSqlWithArrayTest - exit");
    }

    @Test
    public void testConfigOptionsConstructor() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("default_database", "test_db");
        configOptions.put("postgres_jdbc_connection_string", "jdbc:postgresql://hostname:5432/test_db");

        try (MockedStatic<JDBCUtil> jdbcUtilMock = Mockito.mockStatic(JDBCUtil.class)) {
            DatabaseConnectionConfig mockConfig = new DatabaseConnectionConfig("default", POSTGRES_NAME,
                    "jdbc:postgresql://hostname:5432/test_db");
            jdbcUtilMock.when(() -> JDBCUtil.getSingleDatabaseConfigFromEnv(POSTGRES_NAME, configOptions))
                    .thenReturn(mockConfig);

            PostGreSqlRecordHandler handler = new PostGreSqlRecordHandler(configOptions);
            Assert.assertNotNull(handler);
        }
    }

    @Test
    public void buildSqlWithComplexExpressionsTest()
            throws SQLException
    {
        Map<String, ArrowType> fieldTypes = new ImmutableMap.Builder<String, ArrowType>()
                .put(COL_INT, Types.MinorType.INT.getType())
                .put(COL_STRING, Types.MinorType.VARCHAR.getType())
                .put(COL_FLOAT, Types.MinorType.FLOAT8.getType())
                .put(COL_BOOL, Types.MinorType.BIT.getType())
                .build();
        
        Schema schema = createTypedSchema(fieldTypes);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        ValueSet intMultiValues = createMultiValueSet(Arrays.asList(1, 2, 3, 5, 8));
        ValueSet stringLikeValue = getSingleValueSet("test%");
        ValueSet floatRangeValue = getRangeSet(Marker.Bound.ABOVE, 1.5, Marker.Bound.EXACTLY, 10.0);
        ValueSet boolValue = getSingleValueSet(true);

        Map<String, ValueSet> constraints = new ImmutableMap.Builder<String, ValueSet>()
                .put(COL_INT, intMultiValues)
                .put(COL_STRING, stringLikeValue)
                .put(COL_FLOAT, floatRangeValue)
                .put(COL_BOOL, boolValue)
                .build();

        String expectedSql = String.format("SELECT \"%s\", \"%s\", \"%s\", \"%s\" FROM \"%s\".\"%s\"  WHERE (\"%s\" IN (?,?,?,?,?)) AND (\"%s\" = ?) AND ((\"%s\" > ? AND \"%s\" <= ?)) AND (\"%s\" = ?)",
                COL_INT, COL_STRING, COL_FLOAT, COL_BOOL, PARTITION_SCHEMA, PARTITION_NAME,
                COL_INT, COL_STRING, COL_FLOAT, COL_FLOAT, COL_BOOL);
        
        PreparedStatement preparedStatement = executeAndVerifySqlGeneration(
                tableName, schema, createConstraints(constraints), expectedSql);

        // Verify all parameters are set correctly
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(3, 3);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(4, 5);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(5, 8);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(6, "test%");
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(7, 1.5);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(8, 10.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBoolean(9, true);
    }

    @Test
    public void buildSqlWithRangeAndInPredicatesTest()
            throws SQLException
    {
        logger.info("testComplexExpressionWithRangeAndInPredicates - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_DATE, Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_CATEGORY, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_SCORE, new ArrowType.Decimal(10, 2, 128)).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        // Complex constraints: date range, multiple categories, score bounds  
        final long dateStart = TimeUnit.MILLISECONDS.toDays(Date.valueOf("2022-12-31").getTime());
        final long dateEnd = TimeUnit.MILLISECONDS.toDays(Date.valueOf("2023-12-30").getTime());
        ValueSet dateRange = getRangeSet(Marker.Bound.EXACTLY, dateStart, Marker.Bound.EXACTLY, dateEnd);
        ValueSet categories = createMultiValueSet(Arrays.asList("A", "B", "Premium"));
        ValueSet scoreRange = getRangeSet(Marker.Bound.EXACTLY, BigDecimal.valueOf(85.0), Marker.Bound.BELOW, BigDecimal.valueOf(100.0));

        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put(COL_DATE, dateRange)
                .put(COL_CATEGORY, categories)
                .put(COL_SCORE, scoreRange)
                .build());

        String expectedSql = String.format("SELECT \"%s\", \"%s\", \"%s\" FROM \"%s\".\"%s\"  WHERE ((\"%s\" >= ? AND \"%s\" <= ?)) AND (\"%s\" IN (?,?,?)) AND ((\"%s\" >= ? AND \"%s\" < ?))",
                COL_DATE, COL_CATEGORY, COL_SCORE, PARTITION_SCHEMA, PARTITION_NAME,
                COL_DATE, COL_DATE, COL_CATEGORY, COL_SCORE, COL_SCORE);
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify complex parameter setting
        Mockito.verify(preparedStatement, Mockito.times(1)).setDate(1, Date.valueOf("2022-12-31"));
        Mockito.verify(preparedStatement, Mockito.times(1)).setDate(2, Date.valueOf("2023-12-30"));
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "A");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, "B");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(5, "Premium");
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(6, BigDecimal.valueOf(85.0));
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(7, BigDecimal.valueOf(100.0));

        logger.info("testComplexExpressionWithRangeAndInPredicates - exit");
    }

    @Test
    public void buildSqlWithNullableComparisonsTest()
            throws SQLException
    {
        logger.info("testComplexExpressionWithNullableComparisons - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_NULLABLE, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_REQUIRED, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        ValueSet stringValue = getSingleValueSet("test");
        ValueSet requiredValue = getSingleValueSet(42);

        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put(COL_NULLABLE, stringValue)
                .put(COL_REQUIRED, requiredValue)
                .build());

        String expectedSql = String.format("SELECT \"%s\", \"%s\" FROM \"%s\".\"%s\"  WHERE (\"%s\" = ?) AND (\"%s\" = ?)",
                COL_NULLABLE, COL_REQUIRED, PARTITION_SCHEMA, PARTITION_NAME, COL_NULLABLE, COL_REQUIRED);
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameters are set correctly
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "test");
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 42);

        logger.info("testComplexExpressionWithNullableComparisons - exit");
    }

    @Test
    public void testComplexExpressionWithDifferentDataTypes()
            throws SQLException
    {
        logger.info("testComplexExpressionWithDifferentDataTypes - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("tinyintCol", Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("smallintCol", Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("bigintCol", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("float4Col", Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("float8Col", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("decimalCol", new ArrowType.Decimal(10, 2, 128)).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        ValueSet tinyintRange = getRangeSet(Marker.Bound.EXACTLY, (byte) 1, Marker.Bound.BELOW, (byte) 100);
        ValueSet smallintRange = getRangeSet(Marker.Bound.ABOVE, (short) 1000, Marker.Bound.EXACTLY, (short) 5000);
        ValueSet bigintRange = getRangeSet(Marker.Bound.EXACTLY, 1000000L, Marker.Bound.BELOW, 2000000L);
        ValueSet float4Range = getRangeSet(Marker.Bound.ABOVE, 1.5f, Marker.Bound.EXACTLY, 99.9f);
        ValueSet float8Range = getRangeSet(Marker.Bound.EXACTLY, 100.001, Marker.Bound.BELOW, 999.999);
        ValueSet decimalValue = getSingleValueSet(BigDecimal.valueOf(123.45));

        Constraints constraints = createConstraints(new ImmutableMap.Builder<String, ValueSet>()
                .put("tinyintCol", tinyintRange)
                .put("smallintCol", smallintRange)
                .put("bigintCol", bigintRange)
                .put("float4Col", float4Range)
                .put("float8Col", float8Range)
                .put("decimalCol", decimalValue)
                .build());

        String expectedSql = "SELECT \"tinyintCol\", \"smallintCol\", \"bigintCol\", \"float4Col\", \"float8Col\", \"decimalCol\" FROM \"s0\".\"p0\"  WHERE ((\"tinyintCol\" >= ? AND \"tinyintCol\" < ?)) AND ((\"smallintCol\" > ? AND \"smallintCol\" <= ?)) AND ((\"bigintCol\" >= ? AND \"bigintCol\" < ?)) AND ((\"float4Col\" > ? AND \"float4Col\" <= ?)) AND ((\"float8Col\" >= ? AND \"float8Col\" < ?)) AND (\"decimalCol\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify different data type parameter setting
        Mockito.verify(preparedStatement, Mockito.times(1)).setByte(1, (byte) 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setByte(2, (byte) 100);
        Mockito.verify(preparedStatement, Mockito.times(1)).setShort(3, (short) 1000);
        Mockito.verify(preparedStatement, Mockito.times(1)).setShort(4, (short) 5000);
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(5, 1000000L);
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(6, 2000000L);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(7, 1.5f);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(8, 99.9f);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(9, 100.001);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(10, 999.999);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(11, BigDecimal.valueOf(123.45));

        logger.info("testComplexExpressionWithDifferentDataTypes - exit");
    }

    @Test
    public void buildSqlWithLimitTest()
            throws SQLException
    {
        Schema schema = createSqlTestSchema(COL_ID, COL_NAME);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                LIMIT_100,
                Collections.emptyMap(),
                null
        );

        String expectedSql = String.format("SELECT \"%s\", \"%s\" FROM \"%s\".\"%s\"  LIMIT %d", 
                COL_ID, COL_NAME, PARTITION_SCHEMA, PARTITION_NAME, LIMIT_100);
        
        executeAndVerifySqlGeneration(tableName, schema, constraints, expectedSql);
    }

    @Test
    public void testLimitWithDifferentValues()
            throws SQLException
    {
        logger.info("testLimitWithDifferentValues - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        long[] limitValues = {1L, 10L, 50L, 1000L, 10000L};

        for (long limitValue : limitValues) {
            Constraints constraints = new Constraints(
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    limitValue,
                    Collections.emptyMap(),
                    null
            );

            String expectedSql = "SELECT \"col1\" FROM \"s0\".\"p0\"  LIMIT " + limitValue;
            PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

            PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

            Assert.assertEquals("Failed for LIMIT " + limitValue, expectedPreparedStatement, preparedStatement);
        }

        logger.info("testLimitWithDifferentValues - exit");
    }

    @Test
    public void testLimitWithConstraints()
            throws SQLException
    {
        logger.info("testLimitWithConstraints - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("status", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        // Create constraints with both WHERE conditions and LIMIT
        ValueSet idRange = getRangeSet(Marker.Bound.EXACTLY, 1, Marker.Bound.BELOW, 1000);
        ValueSet statusValues = createMultiValueSet(Arrays.asList("ACTIVE", "PENDING"));

        Constraints constraints = new Constraints(
                new ImmutableMap.Builder<String, ValueSet>()
                        .put(COL_ID, idRange)
                        .put("status", statusValues)
                        .build(),
                Collections.emptyList(),
                Collections.emptyList(),
                LIMIT_25,
                Collections.emptyMap(),
                null
        );

        String expectedSql = String.format("SELECT \"id\", \"status\" FROM \"s0\".\"p0\"  WHERE ((\"id\" >= ? AND \"id\" < ?)) AND (\"status\" IN (?,?)) LIMIT %d", LIMIT_25);
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameters
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 1000);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "ACTIVE");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, "PENDING");

        logger.info("testLimitWithConstraints - exit");
    }

    @Test
    public void buildSqlWithOrderByTest()
            throws SQLException
    {
        Schema schema = createSqlTestSchema(COL_ID, COL_NAME);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        
        List<OrderByField> orderByFields = List.of(
                new OrderByField(COL_NAME, OrderByField.Direction.ASC_NULLS_FIRST)
        );
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                orderByFields,
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = String.format("SELECT \"%s\", \"%s\" FROM \"%s\".\"%s\"  ORDER BY \"%s\" ASC NULLS FIRST",
                COL_ID, COL_NAME, PARTITION_SCHEMA, PARTITION_NAME, COL_NAME);
        
        executeAndVerifySqlGeneration(tableName, schema, constraints, expectedSql);
    }

    @Test
    public void buildSqlWithMultipleOrderByColumnsTest()
            throws SQLException
    {
        logger.info("testOrderByMultipleColumns - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("category", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("priority", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("created_date", Types.MinorType.DATEDAY.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        List<OrderByField> orderByFields = Arrays.asList(
                new OrderByField("category", OrderByField.Direction.ASC_NULLS_FIRST),
                new OrderByField("priority", OrderByField.Direction.DESC_NULLS_LAST),
                new OrderByField("created_date", OrderByField.Direction.DESC_NULLS_FIRST)
        );

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                orderByFields,
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"category\", \"priority\", \"created_date\" FROM \"s0\".\"p0\"  ORDER BY \"category\" ASC NULLS FIRST, \"priority\" DESC NULLS LAST, \"created_date\" DESC NULLS FIRST";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        logger.info("testOrderByMultipleColumns - exit");
    }

    @Test
    public void testOrderByWithConstraints()
            throws SQLException
    {
        logger.info("testOrderByWithConstraints - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("status", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("score", Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);

        // Create constraints with both WHERE conditions and ORDER BY
        ValueSet idRange = getRangeSet(Marker.Bound.EXACTLY, 1, Marker.Bound.BELOW, 1000);
        ValueSet statusValues = createMultiValueSet(Arrays.asList(STATUS_ACTIVE, STATUS_PENDING));

        List<OrderByField> orderByFields = Arrays.asList(
                new OrderByField("score", OrderByField.Direction.DESC_NULLS_LAST),
                new OrderByField(COL_ID, OrderByField.Direction.ASC_NULLS_FIRST)
        );

        Constraints constraints = new Constraints(
                new ImmutableMap.Builder<String, ValueSet>()
                        .put(COL_ID, idRange)
                        .put("status", statusValues)
                        .build(),
                Collections.emptyList(),
                orderByFields,
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\", \"status\", \"score\" FROM \"s0\".\"p0\"  WHERE ((\"id\" >= ? AND \"id\" < ?)) AND (\"status\" IN (?,?)) ORDER BY \"score\" DESC NULLS LAST, \"id\" ASC NULLS FIRST";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameters
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 1000);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, STATUS_ACTIVE);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, STATUS_PENDING);

        logger.info("testOrderByWithConstraints - exit");
    }

    @Test
    public void buildSqlWithAllFeaturesTest()
            throws SQLException
    {
        Map<String, ArrowType> fieldTypes = new ImmutableMap.Builder<String, ArrowType>()
                .put(COL_ID, Types.MinorType.INT.getType())
                .put(COL_CATEGORY, Types.MinorType.VARCHAR.getType())
                .put(COL_PRICE, new ArrowType.Decimal(10, 2, 128))
                .put(COL_RATING, Types.MinorType.FLOAT8.getType())
                .put(COL_ACTIVE, Types.MinorType.BIT.getType())
                .build();
        
        Schema schema = createTypedSchema(fieldTypes);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        Map<String, ValueSet> whereConstraints = new ImmutableMap.Builder<String, ValueSet>()
                .put(COL_ID, getRangeSet(Marker.Bound.EXACTLY, 100, Marker.Bound.BELOW, 50000))
                .put(COL_CATEGORY, createMultiValueSet(Arrays.asList("Electronics", "Books", "Clothing")))
                .put(COL_PRICE, getRangeSet(Marker.Bound.ABOVE, BigDecimal.valueOf(10.00), Marker.Bound.EXACTLY, BigDecimal.valueOf(500.00)))
                .put(COL_RATING, getRangeSet(Marker.Bound.EXACTLY, 4.0, Marker.Bound.BELOW, 5.0))
                .put(COL_ACTIVE, getSingleValueSet(true))
                .build();
                
        List<OrderByField> orderByFields = Arrays.asList(
                new OrderByField(COL_CATEGORY, OrderByField.Direction.ASC_NULLS_FIRST),
                new OrderByField(COL_RATING, OrderByField.Direction.DESC_NULLS_LAST),
                new OrderByField(COL_PRICE, OrderByField.Direction.ASC_NULLS_LAST)
        );

        Constraints constraints = new Constraints(
                whereConstraints,
                Collections.emptyList(),
                orderByFields,
                LIMIT_20,
                Collections.emptyMap(),
                null
        );

        String expectedSql = String.format("SELECT \"%s\", \"%s\", \"%s\", \"%s\", \"%s\" FROM \"%s\".\"%s\"  WHERE ((\"%s\" >= ? AND \"%s\" < ?)) AND (\"%s\" IN (?,?,?)) AND ((\"%s\" > ? AND \"%s\" <= ?)) AND ((\"%s\" >= ? AND \"%s\" < ?)) AND (\"%s\" = ?) ORDER BY \"%s\" ASC NULLS FIRST, \"%s\" DESC NULLS LAST, \"%s\" ASC NULLS LAST LIMIT %d",
                COL_ID, COL_CATEGORY, COL_PRICE, COL_RATING, COL_ACTIVE, PARTITION_SCHEMA, PARTITION_NAME,
                COL_ID, COL_ID, COL_CATEGORY, COL_PRICE, COL_PRICE, COL_RATING, COL_RATING, COL_ACTIVE,
                COL_CATEGORY, COL_RATING, COL_PRICE, LIMIT_20);
        
        PreparedStatement preparedStatement = executeAndVerifySqlGeneration(
                tableName, schema, constraints, expectedSql);

        // Verify all parameters are set correctly with proper types
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 100);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 50000);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "Electronics");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, "Books");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(5, "Clothing");
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(6, BigDecimal.valueOf(10.00));
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(7, BigDecimal.valueOf(500.00));
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(8, 4.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(9, 5.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBoolean(10, true);
    }

    @Test
    public void testTopNWithOrderByScenario()
            throws SQLException
    {
        logger.info("testTopNWithOrderByScenario - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = createTestSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("employee_id", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("department", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("salary", new ArrowType.Decimal(10, 2, 128)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("hire_date", Types.MinorType.DATEDAY.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);
        ValueSet deptFilter = getSingleValueSet("Engineering");
        ValueSet salaryRange = getRangeSet(Marker.Bound.EXACTLY, BigDecimal.valueOf(50000), Marker.Bound.BELOW, BigDecimal.valueOf(200000));

        List<OrderByField> orderByFields = Arrays.asList(
                new OrderByField("salary", OrderByField.Direction.DESC_NULLS_LAST),
                new OrderByField("hire_date", OrderByField.Direction.ASC_NULLS_FIRST)
        );

        Constraints constraints = new Constraints(
                new ImmutableMap.Builder<String, ValueSet>()
                        .put("department", deptFilter)
                        .put("salary", salaryRange)
                        .build(),
                Collections.emptyList(),
                orderByFields,
                LIMIT_5,
                Collections.emptyMap(),
                null
        );

        String expectedSql = String.format("SELECT \"employee_id\", \"department\", \"salary\", \"hire_date\" FROM \"s0\".\"p0\"  WHERE (\"department\" = ?) AND ((\"salary\" >= ? AND \"salary\" < ?)) ORDER BY \"salary\" DESC NULLS LAST, \"hire_date\" ASC NULLS FIRST LIMIT %d", LIMIT_5);
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameters
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "Engineering");
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(2, BigDecimal.valueOf(50000));
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(3, BigDecimal.valueOf(200000));

        logger.info("testTopNWithOrderByScenario - exit");
    }

    private Constraints createConstraints(Map<String, ValueSet> valueSetMap) {
        return new Constraints(
                valueSetMap,
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );
    }

    private Constraints createConstraints(String columnName, ValueSet valueSet) {
        return createConstraints(Collections.singletonMap(columnName, valueSet));
    }

    private Split createMockSplit(String partitionSchema, String partitionName) {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of(
                PARTITION_SCHEMA_NAME, partitionSchema,
                PARTITION_NAME_COL, partitionName));
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME)))
                .thenReturn(partitionSchema);
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME)))
                .thenReturn(partitionName);
        return split;
    }

    private PreparedStatement createMockPreparedStatement(String expectedSql) throws SQLException {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql)))
                .thenReturn(preparedStatement);
        return preparedStatement;
    }

    private void createMockPreparedStatementWithError(String errorMessage) throws SQLException {
        Mockito.when(this.connection.prepareStatement(Mockito.anyString()))
                .thenThrow(new SQLException(errorMessage));
    }

    private SchemaBuilder createTestSchemaBuilder() {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_SCHEMA_NAME, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NAME_COL, Types.MinorType.VARCHAR.getType()).build());
        return schemaBuilder;
    }

    private ValueSet createMultiValueSet(List<?> values) {
        List<Range> ranges = values.stream().map(value -> {
            Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(range.isSingleValue()).thenReturn(true);
            Mockito.when(range.getLow().getValue()).thenReturn(value);
            return range;
        }).collect(Collectors.toList());

        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(ranges);
        return valueSet;
    }

    private Schema createSqlTestSchema(String... additionalFields) {
        SchemaBuilder schemaBuilder = createTestSchemaBuilder();

        for (String field : additionalFields) {
            schemaBuilder.addField(FieldBuilder.newBuilder(field, Types.MinorType.VARCHAR.getType()).build());
        }

        return schemaBuilder.build();
    }

    private Schema createTypedSchema(Map<String, ArrowType> fieldDefinitions) {
        SchemaBuilder schemaBuilder = createTestSchemaBuilder();

        fieldDefinitions.forEach((fieldName, arrowType) -> schemaBuilder.addField(FieldBuilder.newBuilder(fieldName, arrowType).build()));

        return schemaBuilder.build();
    }

    private PreparedStatement executeAndVerifySqlGeneration(TableName tableName,
                                                            Schema schema,
                                                            Constraints constraints,
                                                            String expectedSql) throws SQLException {
        Split split = createMockSplit(PARTITION_SCHEMA, PARTITION_NAME);
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(
                this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        return preparedStatement;
    }
}

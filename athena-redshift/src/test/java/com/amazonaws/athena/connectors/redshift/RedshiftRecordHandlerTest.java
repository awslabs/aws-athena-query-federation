/*-
 * #%L
 * athena-redshift
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
package com.amazonaws.athena.connectors.redshift;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlQueryStringBuilder;
import com.amazonaws.athena.connectors.postgresql.PostgreSqlFederationExpressionParser;
import com.google.common.collect.ImmutableList;
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
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Date;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;

public class RedshiftRecordHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftRecordHandlerTest.class);
    private MockedStatic<JDBCUtil> mockedJDBCUtil;

    // SQL command constants
    private static final String ENABLE_CASE_SENSITIVE = "SET enable_case_sensitive_identifier to TRUE;";
    private static final String DISABLE_CASE_SENSITIVE = "SET enable_case_sensitive_identifier to FALSE;";

    // Test data constants
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_CONNECTION_STRING = "redshift://jdbc:redshift://hostname/user=A&password=B";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_COL_1 = "testCol1";
    private static final String TEST_COL_2 = "testCol2";
    private static final String TEST_COL_3 = "testCol3";
    private static final String TEST_COL_4 = "testCol4";
    private static final String TEST_COL_5 = "testCol5";
    private static final String TEST_COL_6 = "testCol6";
    private static final String TEST_COL_7 = "testCol7";
    private static final String TEST_COL_8 = "testCol8";
    private static final String TEST_COL_9 = "testCol9";
    private static final String TEST_COL_10 = "testCol10";
    private static final String TEST_DATE = "testDate";
    private static final String PARTITION_SCHEMA_NAME = "partition_schema_name";
    private static final String PARTITION_NAME = "partition_name";
    private static final String S0 = "s0";
    private static final String P0 = "p0";
    private static final String TEST_DATE_VALUE = "2020-01-05";

    // Expected SQL query constants
    private static final String EXPECTED_SQL_WITH_CONSTRAINTS = "SELECT \"testCol1\", \"testCol2\", \"testCol3\", \"testCol4\", \"testCol5\", \"testCol6\", \"testCol7\", \"testCol8\", \"testCol9\", RTRIM(\"testCol10\") AS \"testCol10\" FROM \"s0\".\"p0\"  WHERE (\"testCol1\" IN (?,?)) AND ((\"testCol2\" >= ? AND \"testCol2\" < ?)) AND ((\"testCol3\" > ? AND \"testCol3\" <= ?)) AND (\"testCol4\" = ?) AND (\"testCol5\" = ?) AND (\"testCol6\" = ?) AND (\"testCol7\" = ?) AND (\"testCol8\" = ?) AND (\"testCol9\" = ?) AND (\"testCol10\" = ?)";
    private static final String EXPECTED_SQL_FOR_DATE = "SELECT \"testDate\" FROM \"s0\".\"p0\"  WHERE (\"testDate\" = ?)";

    private RedshiftRecordHandler redshiftRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private MockedStatic<PostGreSqlMetadataHandler> mockedPostGreSqlMetadataHandler;
    private Statement mockStatement;

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
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, REDSHIFT_NAME, TEST_CONNECTION_STRING);

        this.redshiftRecordHandler = new RedshiftRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
        mockedPostGreSqlMetadataHandler = Mockito.mockStatic(PostGreSqlMetadataHandler.class);
        mockedPostGreSqlMetadataHandler.when(() -> PostGreSqlMetadataHandler.getCharColumns(any(), anyString(), anyString())).thenReturn(Collections.singletonList(TEST_COL_10));
        mockStatement = Mockito.mock(Statement.class);
        Mockito.when(connection.createStatement()).thenReturn(mockStatement);
    }

    @After
    public void close(){
        mockedPostGreSqlMetadataHandler.close();
        if (mockedJDBCUtil != null) {
            mockedJDBCUtil.close();
        }
    }

    @Test
    public void testConstructorWithConfigOptions() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("redshift_connection_string", TEST_CONNECTION_STRING);
        DatabaseConnectionConfig expectedConfig = new DatabaseConnectionConfig(TEST_CATALOG, REDSHIFT_NAME, TEST_CONNECTION_STRING);
        mockedJDBCUtil = Mockito.mockStatic(JDBCUtil.class);
        mockedJDBCUtil.when(() -> JDBCUtil.getSingleDatabaseConfigFromEnv(REDSHIFT_NAME, configOptions))
                .thenReturn(expectedConfig);

        RedshiftRecordHandler handler = new RedshiftRecordHandler(configOptions);

        Assert.assertNotNull(handler);
        mockedJDBCUtil.verify(() -> JDBCUtil.getSingleDatabaseConfigFromEnv(REDSHIFT_NAME, configOptions), Mockito.times(1));
    }

    @Test
    public void buildSplitSqlTest()
            throws SQLException
    {
        logger.info("buildSplitSqlTest - enter");

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_2, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_3, Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_4, Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_5, Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_6, Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_7, Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_8, Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_9, new ArrowType.Decimal(8, 2)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL_10, new ArrowType.Utf8()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_SCHEMA_NAME, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NAME, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of(PARTITION_SCHEMA_NAME, S0, PARTITION_NAME, P0));
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn(S0);
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn(P0);

        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.isSingleValue()).thenReturn(true);
        Mockito.when(range1a.getLow().getValue()).thenReturn(1);
        Range range1b = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1b.isSingleValue()).thenReturn(true);
        Mockito.when(range1b.getLow().getValue()).thenReturn(2);
        ValueSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.getRanges().getOrderedRanges()).thenReturn(ImmutableList.of(range1a, range1b));

        ValueSet valueSet2 = getRangeSet(Marker.Bound.EXACTLY, "1", Marker.Bound.BELOW, "10");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.ABOVE, 2L, Marker.Bound.EXACTLY, 20L);
        ValueSet valueSet4 = getSingleValueSet(1.1F);
        ValueSet valueSet5 = getSingleValueSet(1);
        ValueSet valueSet6 = getSingleValueSet(0);
        ValueSet valueSet7 = getSingleValueSet(1.2d);
        ValueSet valueSet8 = getSingleValueSet(true);
        ValueSet valueSet9 = getSingleValueSet(BigDecimal.valueOf(12.34));
        ValueSet valueSet10 = getSingleValueSet("A");

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put(TEST_COL_1, valueSet1)
                .put(TEST_COL_2, valueSet2)
                .put(TEST_COL_3, valueSet3)
                .put(TEST_COL_4, valueSet4)
                .put(TEST_COL_5, valueSet5)
                .put(TEST_COL_6, valueSet6)
                .put(TEST_COL_7, valueSet7)
                .put(TEST_COL_8, valueSet8)
                .put(TEST_COL_9, valueSet9)
                .put(TEST_COL_10, valueSet10)
                .build());

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(EXPECTED_SQL_WITH_CONSTRAINTS))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.redshiftRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertSame(expectedPreparedStatement, preparedStatement);
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

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_DATE, Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_SCHEMA_NAME, Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NAME, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of(PARTITION_SCHEMA_NAME, S0, PARTITION_NAME, P0));
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn(S0);
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn(P0);

        final long dateDays = TimeUnit.MILLISECONDS.toDays(Date.valueOf(TEST_DATE_VALUE).getTime());
        ValueSet valueSet = getSingleValueSet(dateDays);

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(Collections.singletonMap(TEST_DATE, valueSet));

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(EXPECTED_SQL_FOR_DATE))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.redshiftRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Date expectedDate = new Date(120, 0, 5);
        Assert.assertSame(expectedPreparedStatement, preparedStatement);
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

    @Test
    public void testEnableCaseSensitivelyLookUpSessionSuccess() throws SQLException {
        Mockito.when(mockStatement.execute(ENABLE_CASE_SENSITIVE)).thenReturn(true);
        boolean result = redshiftRecordHandler.enableCaseSensitivelyLookUpSession(connection);

        assertTrue(result);
        Mockito.verify(mockStatement, Mockito.times(1)).execute(ENABLE_CASE_SENSITIVE);
    }

    @Test
    public void testEnableCaseSensitivelyLookUpSessionFailure() throws SQLException {
        Mockito.when(mockStatement.execute(ENABLE_CASE_SENSITIVE)).thenThrow(new SQLException("Simulated failure"));
        boolean result = redshiftRecordHandler.enableCaseSensitivelyLookUpSession(connection);

        assertFalse(result);
        Mockito.verify(mockStatement, Mockito.times(1)).execute(ENABLE_CASE_SENSITIVE);
    }

    @Test
    public void testDisableCaseSensitivelyLookUpSessionSuccess() throws SQLException {
        Mockito.when(mockStatement.execute(DISABLE_CASE_SENSITIVE)).thenReturn(true);
        boolean result = redshiftRecordHandler.disableCaseSensitivelyLookUpSession(connection);

        assertTrue(result);
        Mockito.verify(mockStatement, Mockito.times(1)).execute(DISABLE_CASE_SENSITIVE);
    }

    @Test
    public void testDisableCaseSensitivelyLookUpSessionFailure() throws SQLException {
        Mockito.when(mockStatement.execute(DISABLE_CASE_SENSITIVE)).thenThrow(new SQLException("Simulated failure"));
        boolean result = redshiftRecordHandler.disableCaseSensitivelyLookUpSession(connection);

        assertFalse(result);
        Mockito.verify(mockStatement, Mockito.times(1)).execute(DISABLE_CASE_SENSITIVE);
    }
}
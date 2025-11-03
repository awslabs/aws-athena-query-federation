/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
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
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.QUERY;
import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_NAME;
import static org.mockito.ArgumentMatchers.nullable;

public class OracleRecordHandlerTest
{
    private OracleRecordHandler oracleRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    private static final String ORACLE_QUOTE_CHARACTER = "\"";
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_PARTITION = "p0";
    private static final String PARTITION_COLUMN = "partition_name";
    private static final String COL_ID = "id";
    private static final String COL_NAME = "name";
    private static final String VALUE = "value";
    private static final long LIMIT_10 = 10L;
    private static final long LIMIT_5 = 5L;

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
        jdbcSplitQueryBuilder = new OracleQueryStringBuilder(ORACLE_QUOTE_CHARACTER, new OracleFederationExpressionParser(ORACLE_QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, ORACLE_NAME,
                "oracle://jdbc:oracle:thin:username/password@//127.0.0.1:1521/orcl");

        this.oracleRecordHandler = new OracleRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void buildSplitSql_withConstraints_returnsPreparedStatement()
            throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol5", Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol6", Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol7", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol8", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol9", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol10", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("p0");

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
        final long epochDaysPrior1970 = LocalDate.parse("1967-07-27").toEpochDay();
        ValueSet valueSet9 = getSingleValueSet(epochDaysPrior1970);
        final long epochDaysPost1970 = LocalDate.parse("1971-01-01").toEpochDay();
        ValueSet valueSet10 = getSingleValueSet(epochDaysPost1970);

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet1)
                .put("testCol2", valueSet2)
                .put("testCol3", valueSet3)
                .put("testCol4", valueSet4)
                .put("testCol5", valueSet5)
                .put("testCol6", valueSet6)
                .put("testCol7", valueSet7)
                .put("testCol8", valueSet8)
                .put("testCol9", valueSet9)
                .put("testCol10", valueSet10)
                .build());

        Mockito.when(constraints.getLimit()).thenReturn(5L);

        String expectedSql = "SELECT \"testCol1\", \"testCol2\", \"testCol3\", \"testCol4\", \"testCol5\", \"testCol6\", \"testCol7\", \"testCol8\", \"testCol9\", \"testCol10\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  WHERE (\"testCol1\" IN (?,?)) AND ((\"testCol2\" >= ? AND \"testCol2\" < ?)) AND ((\"testCol3\" > ? AND \"testCol3\" <= ?)) AND (\"testCol4\" = ?) AND (\"testCol5\" = ?) AND (\"testCol6\" = ?) AND (\"testCol7\" = ?) AND (\"testCol8\" = ?) AND (\"testCol9\" = ?) AND (\"testCol10\" = ?) FETCH FIRST 5 ROWS ONLY ";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.oracleRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

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
        Date expectedDatePrior1970 = Date.valueOf(LocalDate.of(1967, 7, 27));
        Mockito.verify(preparedStatement, Mockito.times(1)).setDate(12, expectedDatePrior1970);
        Date expectedDatePost1970 = Date.valueOf(LocalDate.of(1971, 1, 1));
        Mockito.verify(preparedStatement, Mockito.times(1)).setDate(13, expectedDatePost1970);
    }

    private ValueSet getSingleValueSet(Object value)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue)
    {
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
    public void buildSplitSql_withComplexExpressions_buildsSQLWithNestedPredicates() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("col2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("col3", Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit();
        ValueSet valueSet1 = getRangeSet(Marker.Bound.ABOVE, 10, Marker.Bound.EXACTLY, 100);
        ValueSet valueSet2 = getRangeSet(Marker.Bound.EXACTLY, "test", Marker.Bound.BELOW, "tesu");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.EXACTLY, 1.0d, Marker.Bound.EXACTLY, 2.0d);

        Constraints constraints = new Constraints(
                new ImmutableMap.Builder<String, ValueSet>()
                        .put("col1", valueSet1)
                        .put("col2", valueSet2)
                        .put("col3", valueSet3)
                        .build(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"col1\", \"col2\", \"col3\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  WHERE ((\"col1\" > ? AND \"col1\" <= ?)) AND ((\"col2\" >= ? AND \"col2\" < ?)) AND ((\"col3\" >= ? AND \"col3\" <= ?))";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.oracleRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 10);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 100);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "test");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, "tesu");
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(5, 1.0d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(6, 2.0d);
    }

    @Test
    public void buildSplitSql_withTopN_includesFetchFirstClause() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithValueField().build();
        Split split = createMockSplit();
        Constraints constraints = createConstraintsWithLimit(LIMIT_10);

        String expectedSql = "SELECT \"id\", \"value\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  FETCH FIRST " + LIMIT_10 + " ROWS ONLY ";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.oracleRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_withOrderBy_includesOrderByClause() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createSchemaWithCommonFields();
        schemaBuilder.addField(FieldBuilder.newBuilder(VALUE, Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();
        Split split = createMockSplit();

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(VALUE, OrderByField.Direction.DESC_NULLS_LAST));
        orderByFields.add(new OrderByField(COL_NAME, OrderByField.Direction.ASC_NULLS_LAST));

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                orderByFields,
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\", \"name\", \"value\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  ORDER BY \"value\" DESC NULLS LAST, \"name\" ASC NULLS LAST";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.oracleRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_withLimitOffset_includesLimitAndOffsetClauses() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithValueField().build();
        Split split = createMockSplit();
        Constraints constraints = createConstraintsWithLimit(LIMIT_5);
        
        String expectedSql = "SELECT \"id\", \"value\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  FETCH FIRST " + LIMIT_5 + " ROWS ONLY ";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.oracleRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_withRangeAndInPredicates_buildsSQLWithCombinedWhereClause() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createBasicSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("intCol", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("doubleCol", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("stringCol", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit();

        ValueSet intValueSet = getSingleValueSet(Arrays.asList(1, 2, 3));
        ValueSet doubleValueSet = getRangeSet(Marker.Bound.EXACTLY, 1.5d, Marker.Bound.BELOW, 5.5d);
        ValueSet stringValueSet = getSingleValueSet(Arrays.asList("value1", "value2"));

        Map<String, ValueSet> summary = new ImmutableMap.Builder<String, ValueSet>()
                .put("intCol", intValueSet)
                .put("doubleCol", doubleValueSet)
                .put("stringCol", stringValueSet)
                .build();

        Constraints constraints = new Constraints(
                summary,
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null);

        String expectedSql = "SELECT \"intCol\", \"doubleCol\", \"stringCol\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  WHERE (\"intCol\" IN (?,?,?)) AND ((\"doubleCol\" >= ? AND \"doubleCol\" < ?)) AND (\"stringCol\" IN (?,?))";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.oracleRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFetchSize(1000);

        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(3, 3);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(4, 1.5d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(5, 5.5d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(6, "value1");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(7, "value2");
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_withDifferentDataTypes_handlesAllDataTypesCorrectly() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createBasicSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("dateCol", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("timestampCol", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("decimalCol", new ArrowType.Decimal(38, 2, 128)).build());
        Schema schema = schemaBuilder.build();
        Split split = createMockSplit();

        ValueSet dateValueSet = getRangeSet(Marker.Bound.EXACTLY, LocalDate.parse("2025-01-01").toEpochDay(), Marker.Bound.EXACTLY, LocalDate.parse("2025-12-31").toEpochDay());

        ValueSet decimalValueSet = getRangeSet(Marker.Bound.ABOVE, new BigDecimal("100.50"), Marker.Bound.BELOW, new BigDecimal("999.99"));

        Map<String, ValueSet> summary = new ImmutableMap.Builder<String, ValueSet>()
                .put("dateCol", dateValueSet)
                .put("decimalCol", decimalValueSet)
                .build();

        Constraints constraints = new Constraints(
                summary,
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null);

        String expectedSql = "SELECT \"dateCol\", \"timestampCol\", \"decimalCol\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  WHERE ((\"dateCol\" >= ? AND \"dateCol\" <= ?)) AND ((\"decimalCol\" > ? AND \"decimalCol\" < ?))";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.oracleRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        
        Mockito.verify(preparedStatement, Mockito.atLeastOnce()).setDate(Mockito.eq(1), Mockito.any(Date.class));
        Mockito.verify(preparedStatement, Mockito.atLeastOnce()).setDate(Mockito.eq(2), Mockito.any(Date.class));
        Mockito.verify(preparedStatement, Mockito.atLeastOnce()).setBigDecimal(Mockito.eq(3), Mockito.eq(new BigDecimal("100.50")));
        Mockito.verify(preparedStatement, Mockito.atLeastOnce()).setBigDecimal(Mockito.eq(4), Mockito.eq(new BigDecimal("999.99")));
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_withQueryPassthrough_returnsPassthroughQuery() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithCommonFields().build();

        Split split = createMockSplit();

        String passthroughQuery = "SELECT * FROM testSchema.testTable WHERE id > 100";
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(QUERY, passthroughQuery);
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                passthroughArgs,
                null);

        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(passthroughQuery);

        PreparedStatement result = this.oracleRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, result);
        verifyFetchSize(expectedPreparedStatement);
    }

    @Test
    public void buildSplitSql_withEmptyConstraints_buildsBasicQuery() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithCommonFields().build();
        Split split = createMockSplit();
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"" + COL_ID + "\", \"" + COL_NAME + "\" FROM \"testSchema\".\"testTable\" PARTITION (p0) ";
        PreparedStatement preparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement result = this.oracleRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(preparedStatement, result);
        verifyFetchSize(preparedStatement);
    }

    @Test
    public void buildSplitSql_withEmptyConstraintsAndOrderBy_buildsQueryWithOrderBy() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = createSchemaWithCommonFields().build();
        Split split = createMockSplit();

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COL_ID, OrderByField.Direction.ASC_NULLS_LAST));
        orderByFields.add(new OrderByField(COL_NAME, OrderByField.Direction.DESC_NULLS_LAST));
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                orderByFields,
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"" + COL_ID + "\", \"" + COL_NAME + "\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  ORDER BY \"" + COL_ID + "\" ASC NULLS LAST, \"" + COL_NAME + "\" DESC NULLS LAST";
        PreparedStatement preparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement result = this.oracleRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals(preparedStatement, result);
        verifyFetchSize(preparedStatement);
    }

    @Test(expected = AthenaConnectorException.class)
    public void buildSplitSql_withInvalidQueryPassthrough_throwsAthenaConnectorException() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        SchemaBuilder schemaBuilder = createBasicSchemaBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createMockSplit();

        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(QUERY, "SELECT * FROM table");
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                passthroughArgs,
                null);

        oracleRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
    }

    private ValueSet getSingleValueSet(List<?> values)
    {
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

    private SchemaBuilder createBasicSchemaBuilder()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_COLUMN, Types.MinorType.VARCHAR.getType()).build());
        return schemaBuilder;
    }

    private SchemaBuilder createSchemaWithCommonFields()
    {
        return createBasicSchemaBuilder()
                .addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder(COL_NAME, Types.MinorType.VARCHAR.getType()).build());
    }

    private SchemaBuilder createSchemaWithValueField()
    {
        return createBasicSchemaBuilder()
                .addField(FieldBuilder.newBuilder(COL_ID, Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder(VALUE, Types.MinorType.FLOAT8.getType()).build());
    }

    private Split createMockSplit()
    {
        Split split = Mockito.mock(Split.class);
        Map<String, String> splitProperties = Collections.singletonMap(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, TEST_PARTITION);
        Mockito.when(split.getProperties()).thenReturn(splitProperties);
        Mockito.when(split.getProperty(Mockito.eq(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn(TEST_PARTITION);
        return split;
    }

    private Constraints createConstraintsWithLimit(long limit)
    {
        return new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                limit,
                Collections.emptyMap(),
                null
        );
    }

    private PreparedStatement createMockPreparedStatement(String expectedSql) throws SQLException
    {
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        return expectedPreparedStatement;
    }

    private void verifyFetchSize(PreparedStatement preparedStatement) throws SQLException
    {
        Mockito.verify(preparedStatement, Mockito.atLeastOnce()).setFetchSize(1000);
    }
}

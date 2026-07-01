/*-
 * #%L
 * athena-mysql
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
package com.amazonaws.athena.connectors.mysql;

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
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField.Direction;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.mysql.MySqlConstants.MYSQL_NAME;
import static org.mockito.ArgumentMatchers.nullable;

public class MySqlRecordHandlerTest
{
    private final String PARTITION_NAME = "partition_name";
    private static final String P0 = "p0";
    private MySqlRecordHandler mySqlRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

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
        jdbcSplitQueryBuilder = new MySqlQueryStringBuilder("`", new MySqlFederationExpressionParser("`"));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", MYSQL_NAME,
                "mysql://jdbc:mysql://hostname/user=A&password=B");

        this.mySqlRecordHandler = new MySqlRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void buildSplitSql_withConstraintsAndSplit_returnsPreparedStatementWithBoundParams()
            throws SQLException
    {
        TableName tableName = getTableName();

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol5", Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol6", Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol7", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol8", Types.MinorType.BIT.getType()).build());
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

        Constraints constraints = getConstraints(valueSet1, Collections.emptyMap());

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `testCol5`, `testCol6`, `testCol7`, `testCol8` FROM `testSchema`.`testTable` PARTITION(p0)  WHERE (`testCol1` IN (?,?)) AND ((`testCol2` >= ? AND `testCol2` < ?)) AND ((`testCol3` > ? AND `testCol3` <= ?)) AND (`testCol4` = ?) AND (`testCol5` = ?) AND (`testCol6` = ?) AND (`testCol7` = ?) AND (`testCol8` = ?) ORDER BY `testCol1` ASC, ISNULL(`testCol3`) DESC, `testCol3` DESC LIMIT 100";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.mySqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

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

        Map<String, String> passthroughArgs = new HashMap<>();
        String qptQuery = "select * from testSchema.testTable";
        passthroughArgs.put("schemaFunctionName", "system.query");
        passthroughArgs.put("QUERY", qptQuery);
        constraints = getConstraints(valueSet1, passthroughArgs);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(qptQuery))).thenReturn(expectedPreparedStatement);
        preparedStatement = this.mySqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_withDateConstraint_generatesSqlWithDatePredicate()
            throws SQLException
    {
        TableName tableName = getTableName();

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testDate", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NAME, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(PARTITION_NAME, P0));
        Mockito.when(split.getProperty(Mockito.eq(PARTITION_NAME))).thenReturn(P0);

        // Convert a date to days since epoch, accounting for timezone
        final java.time.LocalDate localDate = java.time.LocalDate.of(2020, 1, 3);
        final long dateDays = localDate.toEpochDay();
        ValueSet valueSet = getSingleValueSet(dateDays);
        Map<String, ValueSet> constraintMap = Collections.singletonMap("testDate", valueSet);
        Constraints constraints = getDefaultConstraints(constraintMap);

        String expectedSql = "SELECT `testDate` FROM `testSchema`.`testTable` PARTITION(p0)  WHERE (`testDate` = ?)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.mySqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1))
                .setDate(1, Date.valueOf("2020-01-03"));
    }

    @Test(expected = NullPointerException.class)
    public void buildSplitSql_withNullConnection_throwsNullPointerException() throws SQLException {
        TableName tableName = getTableName();
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build())
                .build();
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("p0");
        this.mySqlRecordHandler.buildSplitSql(null, "testCatalogName", tableName, schema, getConstraints(getSingleValueSet(1), Collections.emptyMap()), split);
    }

    @Test(expected = NullPointerException.class)
    public void buildSplitSql_withNullSchema_throwsNullPointerException() throws SQLException {
        TableName tableName = getTableName();
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("p0");
        this.mySqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, null, getConstraints(getSingleValueSet(1), Collections.emptyMap()), split);
    }

    @Test
    public void buildSplitSql_withLargeNumbers_generatesSqlWithDecimalAndBigIntBindings()
            throws SQLException
    {
        TableName tableName = getTableName();

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("bigintCol", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("decimal38_10", new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(38, 10, 128)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("decimal18_6", new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(18, 6, 128)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("decimal9_3", new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(9, 3, 128)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_NAME, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(PARTITION_NAME, P0));
        Mockito.when(split.getProperty(Mockito.eq(PARTITION_NAME))).thenReturn(P0);

        long bigintValue = Long.MAX_VALUE;
        BigDecimal decimal38_10Value = new BigDecimal("12345678901234567890.1234567890");
        BigDecimal decimal18_6Value = new BigDecimal("123456789012.123456");
        BigDecimal decimal9_3Value = new BigDecimal("123456.789");

        Map<String, ValueSet> constraintMap = new ImmutableMap.Builder<String, ValueSet>()
                .put("bigintCol", getSingleValueSet(bigintValue))
                .put("decimal38_10", getSingleValueSet(decimal38_10Value))
                .put("decimal18_6", getSingleValueSet(decimal18_6Value))
                .put("decimal9_3", getSingleValueSet(decimal9_3Value))
                .build();

        Constraints constraints = getDefaultConstraints(constraintMap);

        String expectedSql = "SELECT `bigintCol`, `decimal38_10`, `decimal18_6`, `decimal9_3` FROM `testSchema`.`testTable` PARTITION(p0)  WHERE (`bigintCol` = ?) AND (`decimal38_10` = ?) AND (`decimal18_6` = ?) AND (`decimal9_3` = ?)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.mySqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(1, bigintValue);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(2, decimal38_10Value);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(3, decimal18_6Value);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(4, decimal9_3Value);
    }

    private TableName getTableName() {
        return new TableName("testSchema", "testTable");
    }

    private Constraints getConstraints(ValueSet valueSet1, Map<String, String> queryPassthroughArgs) {
        ValueSet valueSet2 = getRangeSet(Marker.Bound.EXACTLY, "1", Marker.Bound.BELOW, "10");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.ABOVE, 2L, Marker.Bound.EXACTLY, 20L);
        ValueSet valueSet4 = getSingleValueSet(1.1F);
        ValueSet valueSet5 = getSingleValueSet(1);
        ValueSet valueSet6 = getSingleValueSet(0);
        ValueSet valueSet7 = getSingleValueSet(1.2d);
        ValueSet valueSet8 = getSingleValueSet(true);

        return new Constraints(
                new ImmutableMap.Builder<String, ValueSet>()
                        .put("testCol1", valueSet1)
                        .put("testCol2", valueSet2)
                        .put("testCol3", valueSet3)
                        .put("testCol4", valueSet4)
                        .put("testCol5", valueSet5)
                        .put("testCol6", valueSet6)
                        .put("testCol7", valueSet7)
                        .put("testCol8", valueSet8)
                        .build(),
                ImmutableList.of(),
                ImmutableList.of(
                        new OrderByField("testCol1", Direction.ASC_NULLS_FIRST),
                        new OrderByField("testCol3", Direction.DESC_NULLS_FIRST)
                ),
                100L, queryPassthroughArgs, null
        );
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

    private Constraints getDefaultConstraints(Map<String, ValueSet> constraintMap) {
        return new Constraints(
                constraintMap,
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );
    }
}

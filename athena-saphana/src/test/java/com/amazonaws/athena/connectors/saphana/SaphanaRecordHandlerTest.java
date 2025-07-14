/*-
 * #%L
 * athena-saphana
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.saphana;

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
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.SAPHANA_QUOTE_CHARACTER;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;

public class SaphanaRecordHandlerTest
{
    private SaphanaRecordHandler saphanaRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String PARTITION_COLUMN = "partition";
    private static final String PARTITION_VALUE_P0 = "p0";

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
        jdbcSplitQueryBuilder = new SaphanaQueryStringBuilder(SAPHANA_QUOTE_CHARACTER, new SaphanaFederationExpressionParser(SAPHANA_QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SaphanaConstants.SAPHANA_NAME,
                "saphana://jdbc:saphana://115.113.87.100/TMODE=ANSI,CHARSET=UTF8,DATABASE=TEST,USER=DBC,PASSWORD=DBC");

        this.saphanaRecordHandler = new SaphanaRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
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

    private ValueSet getNullOnlyValueSet() {
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.isNone()).thenReturn(true);
        Mockito.when(valueSet.isNullAllowed()).thenReturn(true);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.emptyList());
        return valueSet;
    }

    private ValueSet getNullAllowedValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.isNone()).thenReturn(false);
        Mockito.when(valueSet.isNullAllowed()).thenReturn(true);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void buildSplitSql_withConstraintsAndSplit_returnsPreparedStatementWithBoundParameters()
            throws SQLException
    {
        TableName tableName = createTableName();

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol5", Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol6", Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol7", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol8", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol9", new ArrowType.Decimal(8, 2, 32)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol10", new ArrowType.Binary()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol11", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol12", Types.MinorType.INT.getType()).build());
        schemaBuilder.addStructField("struct")
                .addChildField("struct", "struct_string", Types.MinorType.VARCHAR.getType());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_COLUMN, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createSplitWithPartition(PARTITION_COLUMN, PARTITION_VALUE_P0);

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
        ValueSet valueSet10 = getSingleValueSet(new byte[10]);
        ValueSet valueSet11 = getNullOnlyValueSet(); // Test case for isNone() && isNullAllowed()
        ValueSet valueSet12 = getNullAllowedValueSet(42); // Test case for isNullAllowed()

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
                .put("testCol11", valueSet11)
                .put("testCol12", valueSet12)
                .build());

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.saphanaRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

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
        Mockito.verify(preparedStatement, Mockito.times(1)).setBytes(13, new byte[10]);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(14, 42);
    }

    @Test
    public void buildSplitSql_withDateConstraints_returnsPreparedStatementWithDateBoundParameters() {

        try {
            final String testDateDayCol = "testDateDay";
            final String testDateMilliCol = "testDateMilli";
            final String structCol = "struct";
            final String partition = "partition";
            final String partitionP0 = "P0";
            TableName tableName = createTableName();

            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
            schemaBuilder.addField(FieldBuilder.newBuilder(testDateDayCol, Types.MinorType.DATEDAY.getType()).build());
            schemaBuilder.addField(FieldBuilder.newBuilder(testDateMilliCol, Types.MinorType.DATEMILLI.getType()).build());
            schemaBuilder.addStructField(structCol)
                    .addChildField(structCol, "ST_string()", Types.MinorType.VARCHAR.getType())
                    .addChildField(structCol, "struct_int", Types.MinorType.INT.getType());
            schemaBuilder.addField(FieldBuilder.newBuilder(partition, Types.MinorType.VARCHAR.getType()).build());
            Schema schema = schemaBuilder.build();

            Split split = createSplitWithPartition(partition, partitionP0);

            final long dateDays = TimeUnit.MILLISECONDS.toDays(Date.valueOf("2020-01-05").getTime());
            ValueSet valueSet1 = getSingleValueSet(dateDays);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime timestamp = LocalDateTime.parse("2024-10-03 12:34:56", formatter);
            ValueSet valueSet2 = getSingleValueSet(timestamp);

            Constraints constraints = Mockito.mock(Constraints.class);
            Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                    .put(testDateDayCol, valueSet1)
                    .put(testDateMilliCol, valueSet2)
                    .build());
            List<OrderByField> orderByClause = ImmutableList.of(
                    new OrderByField(testDateDayCol, OrderByField.Direction.ASC_NULLS_FIRST)
            );
            Mockito.when(constraints.getOrderByClause()).thenReturn(orderByClause);
            Mockito.when(constraints.getLimit()).thenReturn(10L);
            PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
            Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);

            PreparedStatement preparedStatement = this.saphanaRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

            Date expectedDate = new Date(TimeUnit.DAYS.toMillis(dateDays));
            Assert.assertEquals(expectedPreparedStatement, preparedStatement);
            Mockito.verify(preparedStatement, Mockito.times(1))
                    .setDate(1, expectedDate);
            Timestamp expectedTimestamp = new Timestamp(timestamp.toInstant(ZoneOffset.UTC).toEpochMilli());
            Mockito.verify(preparedStatement, Mockito.times(1))
                    .setTimestamp(2, expectedTimestamp);
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test
    public void buildSplitSql_withQueryPassthrough_returnsPreparedStatement()
    {
        try {
            Constraints constraints = Mockito.mock(Constraints.class);
            Mockito.when(constraints.isQueryPassThrough()).thenReturn(true);

            String testQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", TEST_SCHEMA, TEST_TABLE, "testCol1");
            Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<String, String>()
                    .put(JdbcQueryPassthrough.QUERY, testQuery)
                    .put("schemaFunctionName", "system.query")
                    .put("enableQueryPassthrough", "true")
                    .put("name", "query")
                    .put("schema", "system")
                    .build();

            Mockito.when(constraints.getQueryPassthroughArguments()).thenReturn(queryPassthroughArgs);

            PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
            Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);

            //For QPT, connection and constraints params are enough.
            PreparedStatement preparedStatement = this.saphanaRecordHandler.buildSplitSql(this.connection, null, null, null, constraints, null);

            Assert.assertEquals(expectedPreparedStatement, preparedStatement);
            Mockito.verify(this.connection).prepareStatement(testQuery);
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test(expected = SQLException.class)
    public void buildSplitSql_whenPrepareStatementThrowsSqlException_throwsSqlException()
            throws SQLException
    {
        TableName tableName = createTableName();
        Schema schema = createMinimalSchemaWithPartition();
        Split split = createSplitWithPartition(PARTITION_COLUMN, PARTITION_VALUE_P0);

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(Collections.emptyMap());

        Mockito.when(this.connection.prepareStatement(Mockito.anyString())).thenThrow(new SQLException("Statement pool exhausted"));

        this.saphanaRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
    }

    @Test
    public void buildSplitSql_withEmptyConstraints_returnsPreparedStatement()
            throws SQLException
    {
        TableName tableName = createTableName();
        Schema schema = createMinimalSchemaWithPartition();
        Split split = createSplitWithPartition(PARTITION_COLUMN, PARTITION_VALUE_P0);

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        Mockito.when(constraints.getOrderByClause()).thenReturn(Collections.emptyList());
        Mockito.when(constraints.getLimit()).thenReturn(0L);

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.anyString())).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.saphanaRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    private TableName createTableName()
    {
        return new TableName(TEST_SCHEMA, TEST_TABLE);
    }

    private Split createSplitWithPartition(String partitionKey, String partitionValue)
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(partitionKey, partitionValue));
        Mockito.when(split.getProperty(Mockito.eq(partitionKey))).thenReturn(partitionValue);
        return split;
    }

    private Schema createMinimalSchemaWithPartition()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(PARTITION_COLUMN, Types.MinorType.VARCHAR.getType()).build());
        return schemaBuilder.build();
    }
}

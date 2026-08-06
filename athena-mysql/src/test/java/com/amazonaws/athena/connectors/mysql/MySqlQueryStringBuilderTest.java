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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.mockito.Mockito.times;

import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.DefaultJdbcFederationExpressionParser;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import java.util.ArrayList;
import java.util.List;

public class MySqlQueryStringBuilderTest
{
    private final String catalogName = "testCatalog";
    private final String schemaName = "testSchema";
    private final String tableName = "testTable";
    private final String TEST_COL1 = "testCol1";
    private final String TEST_COL2 = "testCol2";
    private final String TEST_COL3 = "testCol3";
    private final String TEST_COL4 = "testCol4";
    private final BlockAllocator allocator = new BlockAllocatorImpl("test-allocator-id");
    private final Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
    private final S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
    private final Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null).add("partition_name", "p0");
    private final MySqlQueryStringBuilder mySqlQueryStringBuilder = new MySqlQueryStringBuilder("`", new DefaultJdbcFederationExpressionParser());
    private final Schema schema = SchemaBuilder.newBuilder()
        .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.VARCHAR.getType()).build())
        .addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.INT.getType()).build())
        .addField(FieldBuilder.newBuilder(TEST_COL3, Types.MinorType.VARCHAR.getType()).build())
        .addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.FLOAT8.getType()).build())
        .addField(FieldBuilder.newBuilder("dateCol", Types.MinorType.DATEDAY.getType()).build())
        .addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build())
        .build();

    @Test
    public void buildSql_withNotNullConstraint_generatesPreparedStatementWithIsNotNullPredicate() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(TEST_COL2, SortedRangeSet.of(false, Range.all(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType())));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE (`testCol2` IS NOT NULL)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = mySqlQueryStringBuilder.buildSql(connection, catalogName, tableName, schemaName, schema, constraints, splitBuilder.build());

        assertEquals(expectedPreparedStatement, preparedStatement);

    }

    @Test
    public void buildSql_withNotEqualConstraint_generatesPreparedStatementWithOrPredicate() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(TEST_COL2, SortedRangeSet.of(false, Range.lessThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 138), Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 138)));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`testCol2` < ?) OR (`testCol2` > ?))";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = mySqlQueryStringBuilder.buildSql(connection, catalogName, tableName, schemaName, schema, constraints, splitBuilder.build());

        assertEquals(expectedPreparedStatement, preparedStatement);

    }

    @Test
    public void buildSql_withDateRangeConstraint_generatesPreparedStatementWithDatePredicate() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of("dateCol",
                SortedRangeSet.of(false,
                        Range.lessThan(allocator, Types.MinorType.DATEDAY.getType(), 8035),
                        Range.greaterThan(allocator, Types.MinorType.DATEDAY.getType(), 10440)));

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`dateCol` < ?) OR (`dateCol` > ?))";
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = mySqlQueryStringBuilder.buildSql(connection, catalogName, tableName, schemaName, schema, constraints, splitBuilder.build());

        assertEquals(expectedPreparedStatement, preparedStatement);

        //From sql.Date java doc. Params:
        //year – the year minus 1900; must be 0 to 8099. (Note that 8099 is 9999 minus 1900.)
        //month – 0 to 11
        //day – 1 to 31
        //Start date = 1992-1-1
        Date startDate = new Date(92, 0, 1);
        Mockito.verify(expectedPreparedStatement, times(1)).setDate(1, startDate);
        //End date = 1998-8-2
        Date endDate = new Date(98, 7, 2);
        Mockito.verify(expectedPreparedStatement, times(1)).setDate(2, endDate);
    }

    @Test
    public void buildSql_withOrderByClause_generatesSqlWithOrderByClause() throws Exception
    {
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_LAST));
        orderByFields.add(new OrderByField(TEST_COL2, OrderByField.Direction.DESC_NULLS_LAST));
        Constraints constraints = buildConstraints(Collections.emptyMap(), orderByFields, DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  ORDER BY ISNULL(`testCol1`) ASC, `testCol1` ASC, `testCol2` DESC";
        executeBuildSqlAndVerify(expectedSql, constraints);
    }

    @Test
    public void buildSql_withLimitClause_generatesSqlWithLimitClause() throws Exception
    {
        Constraints constraints = buildConstraints(Collections.emptyMap(), Collections.emptyList(), 100);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  LIMIT 100";
        executeBuildSqlAndVerify(expectedSql, constraints);
    }

    @Test
    public void buildSql_withOrderByAndLimitClause_generatesSqlWithOrderByAndLimit() throws Exception
    {
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_FIRST));
        Constraints constraints = buildConstraints(Collections.emptyMap(), orderByFields, 50);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  ORDER BY `testCol1` ASC LIMIT 50";
        executeBuildSqlAndVerify(expectedSql, constraints);
    }

    @Test
    public void buildSql_withComplexQuery_generatesSqlWithPredicatesOrderByAndLimit() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
                TEST_COL2, SortedRangeSet.of(false, Range.greaterThan(allocator, Types.MinorType.INT.getType(), 50)),
                TEST_COL4, SortedRangeSet.of(false, Range.lessThan(allocator, Types.MinorType.FLOAT8.getType(), 100.5))
        );
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_LAST));
        orderByFields.add(new OrderByField(TEST_COL4, OrderByField.Direction.DESC_NULLS_LAST));
        Constraints constraints = buildConstraints(constraintsMap, orderByFields, 10);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`testCol2` > ?)) AND ((`testCol4` < ?)) ORDER BY ISNULL(`testCol1`) ASC, `testCol1` ASC, `testCol4` DESC LIMIT 10";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setInt(1, 50);
        Mockito.verify(mockPreparedStatement, times(1)).setDouble(2, 100.5);
    }

    @Test
    public void buildSql_withFloatRangeConstraint_generatesSqlWithBoundedFloatPredicate() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
                TEST_COL4, SortedRangeSet.of(false,
                Range.range(allocator, Types.MinorType.FLOAT8.getType(), 10.5, true, 20.7, false))
        );
        Constraints constraints = buildConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`testCol4` >= ? AND `testCol4` < ?))";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setDouble(1, 10.5);
        Mockito.verify(mockPreparedStatement, times(1)).setDouble(2, 20.7);
    }

    @Test
    public void buildSql_withStringRangeConstraint_generatesSqlWithBoundedStringPredicate() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
                TEST_COL1, SortedRangeSet.of(false,
                Range.range(allocator, Types.MinorType.VARCHAR.getType(), "A", true, "B", false))
        );
        Constraints constraints = buildConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`testCol1` >= ? AND `testCol1` < ?))";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setString(1, "A");
        Mockito.verify(mockPreparedStatement, times(1)).setString(2, "B");
    }

    @Test
    public void buildSql_withMultiplePredicates_generatesSqlWithAllPredicates() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
                TEST_COL1, SortedRangeSet.of(false, Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "test")),
                TEST_COL2, SortedRangeSet.of(false, Range.greaterThan(allocator, Types.MinorType.INT.getType(), 100)),
                TEST_COL4, SortedRangeSet.of(false, Range.lessThanOrEqual(allocator, Types.MinorType.FLOAT8.getType(), 50.5)),
            "dateCol", SortedRangeSet.of(false, Range.equal(allocator, Types.MinorType.DATEDAY.getType(), 8035))
        );
        Constraints constraints = buildConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE (`testCol1` = ?) AND ((`testCol2` > ?)) AND ((`testCol4` <= ?)) AND (`dateCol` = ?)";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setString(1, "test");
        Mockito.verify(mockPreparedStatement, times(1)).setInt(2, 100);
        Mockito.verify(mockPreparedStatement, times(1)).setDouble(3, 50.5);
        Mockito.verify(mockPreparedStatement, times(1)).setDate(4, Date.valueOf(LocalDate.of(1992, 1, 1)));
    }

    @Test
    public void buildSql_withEmptyConstraints_generatesBaseSelectSql() throws Exception
    {
        Constraints constraints = buildConstraints(Collections.emptyMap(), Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0) ";
        executeBuildSqlAndVerify(expectedSql, constraints);
    }

    @Test
    public void buildSql_withZeroLimit_generatesBaseSelectSqlWithoutLimit() throws Exception
    {
        Constraints constraints = buildConstraints(Collections.emptyMap(), Collections.emptyList(), 0);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0) ";
        executeBuildSqlAndVerify(expectedSql, constraints);
    }

    @Test
    public void buildSql_withBetweenPredicates_generatesSqlWithBetweenClauses() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
                TEST_COL2, SortedRangeSet.of(false, Range.range(allocator, Types.MinorType.INT.getType(), 10, true, 20, true)),
                TEST_COL4, SortedRangeSet.of(false, Range.range(allocator, Types.MinorType.FLOAT8.getType(), 15.5, true, 25.5, true))
        );
        Constraints constraints = buildConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`testCol2` >= ? AND `testCol2` <= ?)) AND ((`testCol4` >= ? AND `testCol4` <= ?))";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setInt(1, 10);
        Mockito.verify(mockPreparedStatement, times(1)).setInt(2, 20);
        Mockito.verify(mockPreparedStatement, times(1)).setDouble(3, 15.5);
        Mockito.verify(mockPreparedStatement, times(1)).setDouble(4, 25.5);
    }

    @Test
    public void buildSql_withInPredicateMultipleValues_generatesSqlWithInClauses() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
                TEST_COL1, SortedRangeSet.of(false,
                Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "value1"),
                Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "value2"),
                Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "value3")),
                TEST_COL2, SortedRangeSet.of(false,
                Range.equal(allocator, Types.MinorType.INT.getType(), 100),
                Range.equal(allocator, Types.MinorType.INT.getType(), 200),
                Range.equal(allocator, Types.MinorType.INT.getType(), 300))
        );
        Constraints constraints = buildConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE (`testCol1` IN (?,?,?)) AND (`testCol2` IN (?,?,?))";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setString(1, "value1");
        Mockito.verify(mockPreparedStatement, times(1)).setString(2, "value2");
        Mockito.verify(mockPreparedStatement, times(1)).setString(3, "value3");
        Mockito.verify(mockPreparedStatement, times(1)).setInt(4, 100);
        Mockito.verify(mockPreparedStatement, times(1)).setInt(5, 200);
        Mockito.verify(mockPreparedStatement, times(1)).setInt(6, 300);
    }

    @Test
    public void buildSql_withComplexOrderByNullsAndLimit_generatesSqlWithNullsOrderingAndLimit() throws Exception
    {
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_FIRST));
        orderByFields.add(new OrderByField(TEST_COL2, OrderByField.Direction.DESC_NULLS_FIRST));
        orderByFields.add(new OrderByField(TEST_COL4, OrderByField.Direction.ASC_NULLS_LAST));
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
            TEST_COL3, SortedRangeSet.of(false, Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "filter"))
        );
        Constraints constraints = buildConstraints(constraintsMap, orderByFields, 25);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE (`testCol3` = ?) ORDER BY `testCol1` ASC, ISNULL(`testCol2`) DESC, `testCol2` DESC, ISNULL(`testCol4`) ASC, `testCol4` ASC LIMIT 25";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setString(1, "filter");
    }

    @Test
    public void buildSql_withDateRangesMultipleConditions_generatesSqlWithOrDateRanges() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
            "dateCol", SortedRangeSet.of(false,
                Range.range(allocator, Types.MinorType.DATEDAY.getType(), 8035, true, 8040, true),
                Range.range(allocator, Types.MinorType.DATEDAY.getType(), 8070, true, 8075, true))
        );
        Constraints constraints = buildConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`dateCol` >= ? AND `dateCol` <= ?) OR (`dateCol` >= ? AND `dateCol` <= ?))";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setDate(1, Date.valueOf(LocalDate.of(1992, 1, 1)));
        Mockito.verify(mockPreparedStatement, times(1)).setDate(2, Date.valueOf(LocalDate.of(1992, 1, 6)));
        Mockito.verify(mockPreparedStatement, times(1)).setDate(3, Date.valueOf(LocalDate.of(1992, 2, 5)));
        Mockito.verify(mockPreparedStatement, times(1)).setDate(4, Date.valueOf(LocalDate.of(1992, 2, 10)));
    }

    @Test
    public void buildSql_withMixedComparisonOperators_generatesSqlWithOrPredicates() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of(
                TEST_COL2, SortedRangeSet.of(false,
                Range.greaterThan(allocator, Types.MinorType.INT.getType(), 100),
                Range.lessThan(allocator, Types.MinorType.INT.getType(), 50)),
                TEST_COL4, SortedRangeSet.of(false,
                Range.greaterThanOrEqual(allocator, Types.MinorType.FLOAT8.getType(), 75.5),
                Range.lessThanOrEqual(allocator, Types.MinorType.FLOAT8.getType(), 25.5))
        );
        Constraints constraints = buildConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `dateCol` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`testCol2` < ?) OR (`testCol2` > ?)) AND ((`testCol4` <= ?) OR (`testCol4` >= ?))";
        PreparedStatement mockPreparedStatement = executeBuildSqlAndVerify(expectedSql, constraints);

        Mockito.verify(mockPreparedStatement, times(1)).setInt(1, 50);
        Mockito.verify(mockPreparedStatement, times(1)).setInt(2, 100);
        Mockito.verify(mockPreparedStatement, times(1)).setDouble(3, 25.5);
        Mockito.verify(mockPreparedStatement, times(1)).setDouble(4, 75.5);
    }

    // Helper method to create Constraints
    private Constraints buildConstraints(Map<String, ValueSet> constraintsMap, List<OrderByField> orderByFields, long limit)
    {
        return new Constraints(constraintsMap, Collections.emptyList(), orderByFields, limit, Collections.emptyMap(), null);
    }

    // Helper method to execute buildSql and verify the expected SQL
    private PreparedStatement executeBuildSqlAndVerify(String expectedSql, Constraints constraints) throws Exception
    {
        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(expectedSql)).thenReturn(mockPreparedStatement);
        mySqlQueryStringBuilder.buildSql(connection, catalogName, tableName, schemaName, schema, constraints, splitBuilder.build());
        Mockito.verify(connection).prepareStatement(expectedSql);
        return mockPreparedStatement;
    }
}

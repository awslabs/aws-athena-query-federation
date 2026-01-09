/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcQueryBuilderTest
{
    private static final String QUOTE_CHAR = "\"";
    private static final String TEST_SCHEMA = "my_schema";
    private static final String TEST_TABLE = "my_table";
    private static final String TEST_CATALOG = "my_catalog";
    private static final String COL1 = "col1";
    private static final String COL2 = "col2";
    private static final String PARTITION_COL = "partition_col";
    private static final String PARTITION_VALUE = "partition_value";
    
    private JdbcPredicateBuilder mockPredicateBuilder;
    private ST selectQueryTemplate;

    private class TestJdbcQueryBuilder extends JdbcQueryBuilder<TestJdbcQueryBuilder>
    {
        public TestJdbcQueryBuilder(ST template, String quoteChar)
        {
            super(template, quoteChar);
        }

        @Override
        protected JdbcPredicateBuilder createPredicateBuilder()
        {
            return mockPredicateBuilder;
        }
    }

    @Before
    public void setup()
    {
        mockPredicateBuilder = mock(JdbcPredicateBuilder.class);
        selectQueryTemplate = new ST("SELECT <builder.projection; separator=\", \"> FROM <if(builder.catalog)><builder.catalog>.<endif><if(builder.schemaName)><builder.schemaName>.<endif><builder.tableName><if(builder.conjuncts)> WHERE <builder.conjuncts; separator=\" AND \"><endif><if(builder.orderByClause)> <builder.orderByClause><endif><if(builder.limitClause)> <builder.limitClause><endif>");
    }
    
    @Test
    public void build_FullQueryWithAllClauses_ReturnsCompleteSql()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createTwoColumnSchema();
        Split split = createEmptySplit();
        Constraints constraints = createConstraints(
                ImmutableList.of(
                        new OrderByField(COL1, OrderByField.Direction.ASC_NULLS_LAST)
                ),
                100L
        );

        when(mockPredicateBuilder.buildConjuncts(Mockito.anyList(), Mockito.any(), Mockito.anyList(), Mockito.any()))
                .thenReturn(ImmutableList.of("\"" + COL2 + "\" = ?"));

        String sql = queryBuilder
                .withCatalog(TEST_CATALOG)
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .withOrderByClause(constraints)
                .withLimitClause(constraints)
                .build();

        String expectedSql = "SELECT \"col1\", \"col2\" FROM \"my_catalog\".\"my_schema\".\"my_table\" WHERE \"col2\" = ? ORDER BY \"col1\" ASC NULLS LAST  LIMIT 100";
        Assert.assertEquals(expectedSql, sql);
    }

    @Test
    public void build_QueryWithOnlyProjection_ReturnsBasicSql()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createSingleColumnSchema();
        Split split = createEmptySplit();

        String sql = queryBuilder
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .build();

        String expectedSql = "SELECT \"col1\" FROM \"my_schema\".\"my_table\"";
        Assert.assertEquals(expectedSql, sql);
    }

    @Test
    public void extractOrderByClause_MultipleFields_ReturnsCorrectOrderBySql()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);
        
        Constraints constraints = createConstraints(
                ImmutableList.of(
                        new OrderByField(COL1, OrderByField.Direction.ASC_NULLS_FIRST),
                        new OrderByField(COL2, OrderByField.Direction.DESC_NULLS_LAST)
                ),
                Constraints.DEFAULT_NO_LIMIT
        );

        String orderBy = queryBuilder.extractOrderByClause(constraints);
        String expectedOrderBy = "ORDER BY \"col1\" ASC NULLS FIRST, \"col2\" DESC NULLS LAST";
        Assert.assertEquals(expectedOrderBy, orderBy);
    }

    @Test
    public void build_QueryWithOnlyWhereClause_ReturnsSqlWithWhere()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createTwoColumnSchema();
        Split split = createEmptySplit();
        Constraints constraints = createConstraints(Collections.emptyList(),Constraints.DEFAULT_NO_LIMIT);

        when(mockPredicateBuilder.buildConjuncts(Mockito.anyList(), Mockito.any(), Mockito.anyList(), Mockito.any()))
                .thenReturn(ImmutableList.of("\"" + COL2 + "\" = ?"));

        String sql = queryBuilder
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .build();

        String expectedSql = "SELECT \"col1\", \"col2\" FROM \"my_schema\".\"my_table\" WHERE \"col2\" = ?";
        Assert.assertEquals(expectedSql, sql);
    }

    @Test
    public void build_QueryWithOnlyLimit_ReturnsSqlWithLimit()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createSingleColumnSchema();
        Split split = createEmptySplit();
        Constraints constraints = createConstraints(Collections.emptyList(), 50L);

        String sql = queryBuilder
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .withLimitClause(constraints)
                .build();

        String expectedSql = "SELECT \"col1\" FROM \"my_schema\".\"my_table\"  LIMIT 50";
        Assert.assertEquals(expectedSql, sql);
    }

    @Test
    public void build_QueryWithWhereAndLimit_ReturnsSqlWithWhereAndLimit()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createTwoColumnSchema();
        Split split = createEmptySplit();
        Constraints constraints = createConstraints(Collections.emptyList(), 25L);

        when(mockPredicateBuilder.buildConjuncts(Mockito.anyList(), Mockito.any(), Mockito.anyList(), Mockito.any()))
                .thenReturn(ImmutableList.of("\"" + COL1 + "\" > ?"));

        String sql = queryBuilder
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .withLimitClause(constraints)
                .build();

        String expectedSql = "SELECT \"col1\", \"col2\" FROM \"my_schema\".\"my_table\" WHERE \"col1\" > ?  LIMIT 25";
        Assert.assertEquals(expectedSql, sql);
    }

    @Test
    public void build_QueryWithSchemaOnly_ReturnsSqlWithSchema()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createSingleColumnSchema();
        Split split = createEmptySplit();

        String sql = queryBuilder
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .build();

        String expectedSql = "SELECT \"col1\" FROM \"my_schema\".\"my_table\"";
        Assert.assertEquals(expectedSql, sql);
    }

    @Test
    public void extractOrderByClause_SingleField_ReturnsCorrectOrderBySql()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);
        
        Constraints constraints = createConstraints(
                ImmutableList.of(
                        new OrderByField(COL1, OrderByField.Direction.DESC_NULLS_LAST)
                ),
                Constraints.DEFAULT_NO_LIMIT
        );

        String orderBy = queryBuilder.extractOrderByClause(constraints);
        String expectedOrderBy = "ORDER BY \"col1\" DESC NULLS LAST";
        Assert.assertEquals(expectedOrderBy, orderBy);
    }

    @Test
    public void extractOrderByClause_EmptyConstraints_ReturnsEmptyString()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Constraints constraints = createConstraints(Collections.emptyList(),Constraints.DEFAULT_NO_LIMIT);

        String orderBy = queryBuilder.extractOrderByClause(constraints);
        Assert.assertNotNull("OrderBy should not be null", orderBy);
        Assert.assertEquals("", orderBy);
    }

    @Test
    public void withLimitClause_ZeroLimit_DoesNotAddLimitClause()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createSingleColumnSchema();
        Split split = createEmptySplit();
        Constraints constraints = createConstraints(Collections.emptyList(), 0L);

        String sql = queryBuilder
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .withLimitClause(constraints)
                .build();

        String expectedSql = "SELECT \"col1\" FROM \"my_schema\".\"my_table\"";
        Assert.assertEquals(expectedSql, sql);
    }
    
    @Test
    public void withProjection_FiltersPartitionColumns_ExcludesPartitionColumns()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createSchemaWithPartitionColumn();
        Split split = createSplitWithPartition();

        String sql = queryBuilder
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .build();

        String expectedSql = "SELECT \"col1\", \"col2\" FROM \"my_schema\".\"my_table\"";
        Assert.assertEquals(expectedSql, sql);
        Assert.assertFalse("SQL should not contain partition column", sql.contains(PARTITION_COL));
    }

    @Test
    public void getParameterValues_ReturnsEmptyList_Initially()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        List<TypeAndValue> parameterValues = queryBuilder.getParameterValues();
        Assert.assertNotNull("Parameter values should not be null", parameterValues);
        Assert.assertTrue("Parameter values should be empty initially", parameterValues.isEmpty());
    }

    @Test
    public void getCatalog_WithCatalog_ReturnsQuotedCatalog()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);
        queryBuilder.withCatalog(TEST_CATALOG);

        String catalog = queryBuilder.getCatalog();
        Assert.assertEquals("\"" + TEST_CATALOG + "\"", catalog);
    }

    @Test
    public void getCatalog_WithoutCatalog_ReturnsNull()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        String catalog = queryBuilder.getCatalog();
        Assert.assertNull("Catalog should be null when not set", catalog);
    }

    @Test
    public void getSchemaName_WithSchema_ReturnsQuotedSchema()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);
        queryBuilder.withTableName(createStandardTableName());

        String schemaName = queryBuilder.getSchemaName();
        Assert.assertEquals("\"" + TEST_SCHEMA + "\"", schemaName);
    }

    @Test
    public void getTableName_WithTable_ReturnsQuotedTable()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);
        queryBuilder.withTableName(createStandardTableName());

        String tableName = queryBuilder.getTableName();
        Assert.assertEquals("\"" + TEST_TABLE + "\"", tableName);
    }
    
    @Test(expected = NullPointerException.class)
    public void build_NullTableName_ThrowsException()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        Schema schema = createSingleColumnSchema();
        Split split = createEmptySplit();

        queryBuilder
                .withProjection(schema, split)
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void build_NullProjection_ThrowsException()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR);

        queryBuilder
                .withTableName(createStandardTableName())
                .build();
    }

    
    @Test
    public void withConjuncts_NullPredicateBuilder_ReturnsSqlWithoutWhereClause()
    {
        TestJdbcQueryBuilder queryBuilder = new TestJdbcQueryBuilder(selectQueryTemplate, QUOTE_CHAR)
        {
            @Override
            protected JdbcPredicateBuilder createPredicateBuilder()
            {
                return null; // Return null to test null handling
            }
        };

        Schema schema = createSingleColumnSchema();
        Split split = createEmptySplit();
        Constraints constraints = createConstraints(Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT);

        String sql = queryBuilder
                .withTableName(createStandardTableName())
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .build();

        String expectedSql = "SELECT \"col1\" FROM \"my_schema\".\"my_table\"";
        Assert.assertEquals(expectedSql, sql);
        Assert.assertTrue(queryBuilder.getParameterValues().isEmpty());
    }
    
    private Constraints createConstraints(List<OrderByField> orderBy, long limit)
    {
        return new Constraints(
                ImmutableMap.of(),
                Collections.emptyList(),
                orderBy,
                limit,
                Collections.emptyMap(),
                null
        );
    }
    
    private Schema createSingleColumnSchema()
    {
        return new Schema(ImmutableList.of(
                FieldBuilder.newBuilder(COL1, Types.MinorType.INT.getType()).build()
        ));
    }
    
    private Schema createTwoColumnSchema()
    {
        return new Schema(ImmutableList.of(
                FieldBuilder.newBuilder(COL1, Types.MinorType.INT.getType()).build(),
                FieldBuilder.newBuilder(COL2, Types.MinorType.VARCHAR.getType()).build()
        ));
    }
    
    private Schema createSchemaWithPartitionColumn()
    {
        return new Schema(ImmutableList.of(
                FieldBuilder.newBuilder(COL1, Types.MinorType.INT.getType()).build(),
                FieldBuilder.newBuilder(COL2, Types.MinorType.VARCHAR.getType()).build(),
                FieldBuilder.newBuilder(PARTITION_COL, Types.MinorType.VARCHAR.getType()).build()
        ));
    }
    
    private Split createEmptySplit()
    {
        Split split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());
        return split;
    }
    
    private TableName createStandardTableName()
    {
        return new TableName(TEST_SCHEMA, TEST_TABLE);
    }
    
    private Split createSplitWithPartition()
    {
        Split split = mock(Split.class);
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(JdbcQueryBuilderTest.PARTITION_COL, JdbcQueryBuilderTest.PARTITION_VALUE);
        when(split.getProperties()).thenReturn(splitProperties);
        return split;
    }
}

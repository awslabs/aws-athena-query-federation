/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.apache.arrow.vector.types.Types.MinorType.BIT;
import static org.apache.arrow.vector.types.Types.MinorType.DATEDAY;
import static org.apache.arrow.vector.types.Types.MinorType.DATEMILLI;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT4;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT8;
import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.apache.arrow.vector.types.Types.MinorType.SMALLINT;
import static org.apache.arrow.vector.types.Types.MinorType.TINYINT;
import static org.apache.arrow.vector.types.Types.MinorType.VARBINARY;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcSplitQueryBuilderTest
{
    private static final String TEST_CATALOG = "catalog";
    private static final String TEST_SCHEMA = "schema";
    private static final String TEST_TABLE = "table";
    private static final String TEST_COL1 = "col1";
    private static final String TEST_COL_BIGINT = "col_bigint";
    private static final String TEST_COL_INT = "col_int";
    private static final String TEST_COL_SMALLINT = "col_smallint";
    private static final String TEST_COL_TINYINT = "col_tinyint";
    private static final String TEST_COL_FLOAT8 = "col_float8";
    private static final String TEST_COL_FLOAT4 = "col_float4";
    private static final String TEST_COL_BIT = "col_bit";
    private static final String TEST_COL_DATEDAY = "col_dateday";
    private static final String TEST_COL_DATEMILLI = "col_datemilli";
    private static final String TEST_COL_VARCHAR = "col_varchar";
    private static final String TEST_COL_VARBINARY = "col_varbinary";
    private static final String TEST_PARTITION_COL = "partition_col";
    private static final String TEST_PARTITION_VALUE = "2024";
    private static final String QUOTE_CHAR = "\"";

    private BlockAllocatorImpl allocator;
    private JdbcSplitQueryBuilder builder;
    private Connection mockConnection;
    private PreparedStatement mockStatement;
    private Constraints constraints;
    private Split split;
    private Schema schema;
    private FederationExpressionParser expressionParser;

    @Before
    public void setup() throws SQLException
    {
        allocator = new BlockAllocatorImpl();
        expressionParser = mock(FederationExpressionParser.class);
        when(expressionParser.parseComplexExpressions(any(), any(), any())).thenReturn(Collections.emptyList());

        builder = new JdbcSplitQueryBuilder(QUOTE_CHAR, expressionParser)
        {
            @Override
            protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
            {
                return " FROM \"" + schema + "\".\"" + table + "\"";
            }

            @Override
            protected List<String> getPartitionWhereClauses(Split split)
            {
                return Collections.singletonList("\"" + TEST_PARTITION_COL + "\" = '" + TEST_PARTITION_VALUE + "'");
            }
        };

        mockConnection = mock(Connection.class);
        mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());

        constraints = mock(Constraints.class);
        when(constraints.getSummary()).thenReturn(null);
        when(constraints.getOrderByClause()).thenReturn(Collections.emptyList());
        when(constraints.getLimit()).thenReturn(0L);

        Field field = new Field(TEST_COL1, FieldType.nullable(new ArrowType.Int(32, true)), null);
        schema = new Schema(Collections.singletonList(field));
    }

    @After
    public void after() {
        allocator.close();
    }

    @Test
    public void testBuildSql_IntType() throws SQLException
    {
        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraints, split);
        assertNotNull(stmt);
        verify(mockConnection).prepareStatement(contains("SELECT \"" + TEST_COL1 + "\""));
    }

    @Test
    public void testExtractOrderByClause_AscNullsFirst()
    {
        OrderByField orderByField = new OrderByField(TEST_COL1, OrderByField.Direction.ASC_NULLS_FIRST);
        when(constraints.getOrderByClause()).thenReturn(Collections.singletonList(orderByField));

        String clause = builder.extractOrderByClause(constraints);
        assertEquals("ORDER BY \"" + TEST_COL1 + "\" ASC NULLS FIRST", clause);
    }

    @Test
    public void testToPredicateWithSingleValueSet() throws SQLException
    {
        ArrowType type = new ArrowType.Int(32, true);
        SortedRangeSet valueSet = SortedRangeSet.of(false, Range.all(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType()));

        when(constraints.getSummary()).thenReturn(Map.of(TEST_COL1, valueSet));
        Field field = new Field(TEST_COL1, FieldType.nullable(type), null);
        Schema schema = new Schema(List.of(field));

        builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraints, split);
    }

    @Test
    public void testUnsupportedTypeThrowsException()
    {
        // Unsupported ArrowType
        ArrowType unsupportedType = new ArrowType.Duration(TimeUnit.MILLISECOND);

        // Define schema with one unsupported field
        Field unsupportedField = new Field("unsupported", FieldType.nullable(unsupportedType), null);
        Schema schema = new Schema(List.of(unsupportedField));

        // Mock ValueSet with unsupported type
        Range range = Range.equal(allocator, BIGINT.getType(), 123L);
        ValueSet valueSet = SortedRangeSet.of(range);

        // Make constraints map return this ValueSet for the unsupported column
        when(constraints.getSummary()).thenReturn(Map.of("unsupported", valueSet));

        // Execute and verify that the unsupported type triggers exception
        AthenaConnectorException ex = assertThrows(
                AthenaConnectorException.class,
                () -> builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraints, split)
        );

        assertTrue(ex.getMessage().contains("Can't handle type"));
    }

    @Test
    public void testSupportedType() throws SQLException
    {
        List<Field> fields = new ArrayList<>();
        Map<String, ValueSet> valueSetMap = new HashMap<>();

        fields.add(new Field(TEST_COL_BIGINT, FieldType.nullable(BIGINT.getType()), null));
        valueSetMap.put(TEST_COL_BIGINT, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 123L)));

        fields.add(new Field(TEST_COL_INT, FieldType.nullable(INT.getType()), null));
        valueSetMap.put(TEST_COL_INT, SortedRangeSet.of(Range.equal(allocator, INT.getType(), 456)));

        fields.add(new Field(TEST_COL_SMALLINT, FieldType.nullable(SMALLINT.getType()), null));
        valueSetMap.put(TEST_COL_SMALLINT, SortedRangeSet.of(Range.equal(allocator, SMALLINT.getType(), (short) 7)));

        fields.add(new Field(TEST_COL_TINYINT, FieldType.nullable(TINYINT.getType()), null));
        valueSetMap.put(TEST_COL_TINYINT, SortedRangeSet.of(Range.equal(allocator, TINYINT.getType(), (byte) 2)));

        fields.add(new Field(TEST_COL_FLOAT8, FieldType.nullable(FLOAT8.getType()), null));
        valueSetMap.put(TEST_COL_FLOAT8, SortedRangeSet.of(Range.equal(allocator, FLOAT8.getType(), 3.14159d)));

        fields.add(new Field(TEST_COL_FLOAT4, FieldType.nullable(FLOAT4.getType()), null));
        valueSetMap.put(TEST_COL_FLOAT4, SortedRangeSet.of(Range.equal(allocator, FLOAT4.getType(), 2.71f)));

        fields.add(new Field(TEST_COL_BIT, FieldType.nullable(BIT.getType()), null));
        valueSetMap.put(TEST_COL_BIT, SortedRangeSet.of(Range.equal(allocator, BIT.getType(), true)));

        fields.add(new Field(TEST_COL_DATEDAY, FieldType.nullable(DATEDAY.getType()), null));
        valueSetMap.put(TEST_COL_DATEDAY, SortedRangeSet.of(Range.equal(allocator, DATEDAY.getType(), 19000))); // ~2022-01-01

        fields.add(new Field(TEST_COL_DATEMILLI, FieldType.nullable(DATEMILLI.getType()), null));
        valueSetMap.put(TEST_COL_DATEMILLI, SortedRangeSet.of(Range.equal(allocator, DATEMILLI.getType(), LocalDateTime.of(2023, 1, 1, 10, 0))));

        fields.add(new Field(TEST_COL_VARCHAR, FieldType.nullable(VARCHAR.getType()), null));
        valueSetMap.put(TEST_COL_VARCHAR, SortedRangeSet.of(Range.equal(allocator, VARCHAR.getType(), "hello")));

        fields.add(new Field(TEST_COL_VARBINARY, FieldType.nullable(VARBINARY.getType()), null));
        valueSetMap.put(TEST_COL_VARBINARY, SortedRangeSet.of(Range.equal(allocator, VARBINARY.getType(), "abc".getBytes(StandardCharsets.UTF_8))));

        Schema schema = new Schema(fields);
        when(constraints.getSummary()).thenReturn(valueSetMap);

        PreparedStatement stmt = builder.buildSql(mockConnection, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, schema, constraints, split);
        assertNotNull("Generated statement should not be null", stmt);
        // Verify SQL contains all columns
        for (Field field : fields) {
            verify(mockConnection).prepareStatement(contains("\"" + field.getName() + "\""));
        }

        verify(mockConnection).prepareStatement(contains("FROM \"" + TEST_SCHEMA + "\".\"" + TEST_TABLE + "\""));
    }
}

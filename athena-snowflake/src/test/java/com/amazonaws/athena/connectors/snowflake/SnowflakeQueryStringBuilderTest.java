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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnowflakeQueryStringBuilderTest
{
    private SnowflakeQueryStringBuilder queryBuilder;
    private static final String QUOTE_CHARACTER = "\"";
    private static final BlockAllocator blockAllocator = new BlockAllocatorImpl();

    @Before
    public void setUp()
    {
        SnowflakeFederationExpressionParser expressionParser = new SnowflakeFederationExpressionParser(QUOTE_CHARACTER);
        queryBuilder = new SnowflakeQueryStringBuilder(QUOTE_CHARACTER, expressionParser);
    }

    @Test
    public void testGetFromClauseWithSplit()
    {
        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        ).build();
        
        String result = queryBuilder.getFromClauseWithSplit("testCatalog", "testSchema", "testTable", split);
        assertTrue(result.contains("\"testSchema\""));
        assertTrue(result.contains("\"testTable\""));
    }

    @Test
    public void testGetFromClauseWithSplitNoSchema()
    {
        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        ).build();
        
        String result = queryBuilder.getFromClauseWithSplit("testCatalog", null, "testTable", split);
        assertTrue(result.contains("\"testTable\""));
        assertTrue(!result.contains("null"));
    }

    @Test
    public void testQuote()
    {
        String result = queryBuilder.quote("testIdentifier");
        assertEquals("\"testIdentifier\"", result);
    }

    @Test
    public void testBuildSqlWithSimpleConstraints() throws SQLException
    {
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col2", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                Arrays.asList(Range.equal(blockAllocator, Types.MinorType.INT.getType(), 42)), false));

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), -1L, Collections.emptyMap(), null);

        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        ).add("partition", "test-partition").build();

        PreparedStatement result = queryBuilder.buildSql(
                mockConnection,
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints,
                split
        );

        assertNotNull(result);
    }

    @Test
    public void testBuildSqlWithOrderBy() throws SQLException
    {
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        List<OrderByField> orderByFields = Arrays.asList(
                new OrderByField("col1", OrderByField.Direction.ASC_NULLS_FIRST),
                new OrderByField("col2", OrderByField.Direction.DESC_NULLS_LAST)
        );

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), orderByFields, -1L, Collections.emptyMap(), null);

        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        ).add("partition", "test-partition").build();

        PreparedStatement result = queryBuilder.buildSql(
                mockConnection,
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints,
                split
        );

        assertNotNull(result);
    }

    @Test
    public void testBuildSqlWithLimit() throws SQLException
    {
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 100L, Collections.emptyMap(), null);

        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        ).add("partition", "test-partition").build();

        PreparedStatement result = queryBuilder.buildSql(
                mockConnection,
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints,
                split
        );

        assertNotNull(result);
    }

    @Test
    public void testBuildSqlWithPartitionConstraints() throws SQLException
    {
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), -1L, Collections.emptyMap(), null);

        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        )
        .add("partition", "partition-primary--limit-1000-offset-0")
        .build();

        PreparedStatement result = queryBuilder.buildSql(
                mockConnection,
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints,
                split
        );

        assertNotNull(result);
    }

    @Test
    public void testGetBaseExportSQLString() throws SQLException {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .addStringField("partition") // Should be excluded
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), -1L, Collections.emptyMap(), null);

        String result = queryBuilder.getBaseExportSQLString(
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints
        );

        assertNotNull(result);
        assertTrue(result.contains("SELECT"));
        assertTrue(result.contains("\"col1\""));
        assertTrue(result.contains("\"col2\""));
        assertTrue(result.contains("FROM"));
        assertTrue(result.contains("\"testSchema\".\"testTable\""));
        // Should not contain partition column
        assertTrue(!result.contains("\"partition\""));
    }

    @Test
    public void testGetBaseExportSQLStringWithConstraints() throws SQLException {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col2", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                Arrays.asList(Range.greaterThan(blockAllocator, Types.MinorType.INT.getType(), 10)), false));

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), 100L, Collections.emptyMap(), null);

        String result = queryBuilder.getBaseExportSQLString(
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints
        );

        assertNotNull(result);
        assertTrue(result.contains("WHERE"));
        assertTrue(result.contains("LIMIT"));
    }

    @Test
    public void testGetBaseExportSQLStringWithOrderBy() throws SQLException {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        List<OrderByField> orderByFields = Arrays.asList(
                new OrderByField("col1", OrderByField.Direction.ASC_NULLS_FIRST)
        );

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), orderByFields, -1L, Collections.emptyMap(), null);

        String result = queryBuilder.getBaseExportSQLString(
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints
        );

        assertNotNull(result);
        assertTrue(result.contains("ORDER BY"));
        assertTrue(result.contains("\"col1\""));
    }

    @Test
    public void testGetBaseExportSQLStringNoCatalog() throws SQLException {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), -1L, Collections.emptyMap(), null);

        String result = queryBuilder.getBaseExportSQLString(
                null,
                "testSchema",
                "testTable",
                schema,
                constraints
        );

        assertNotNull(result);
        assertTrue(result.contains("\"testSchema\".\"testTable\""));
        assertTrue(!result.contains("null"));
    }

    @Test
    public void testBuildSqlWithComplexPartition() throws SQLException
    {
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), -1L, Collections.emptyMap(), null);

        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        )
        .add("partition", "partition-primary-\"id\",\"name\"-limit-5000-offset-10000")
        .build();

        PreparedStatement result = queryBuilder.buildSql(
                mockConnection,
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints,
                split
        );

        assertNotNull(result);
    }

    @Test
    public void testBuildSqlWithAllPartition() throws SQLException
    {
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addIntField("col2")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), -1L, Collections.emptyMap(), null);

        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        )
        .add("partition", "*") // All partitions
        .build();

        PreparedStatement result = queryBuilder.buildSql(
                mockConnection,
                "testCatalog",
                "testSchema",
                "testTable",
                schema,
                constraints,
                split
        );

        assertNotNull(result);
    }
}
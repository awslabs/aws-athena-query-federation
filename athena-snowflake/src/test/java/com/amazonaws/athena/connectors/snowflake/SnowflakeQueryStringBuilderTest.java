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
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.ALL_PARTITIONS;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.PARTITION_BUCKET_TEMPLATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

    private static Split partitionSplit(String partitionVal)
    {
        return Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        ).add(BLOCK_PARTITION_COLUMN_NAME, partitionVal).build();
    }

    /** Builds a split carrying the hash bucket partition value SnowflakeMetadataHandler produces. */
    private static Split bucketSplit(int bucket, int bucketCount, String quotedKey)
    {
        return partitionSplit(String.format(PARTITION_BUCKET_TEMPLATE, bucket, bucketCount, quotedKey));
    }

    private static String capturePreparedSql(Connection mockConnection) throws SQLException
    {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture());
        return sqlCaptor.getValue();
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

        Split split = bucketSplit(0, 4, "\"col1\"");

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
        // The split's bucket predicate must survive alongside the pushed down filter, otherwise every split
        // would read the whole table.
        String sql = capturePreparedSql(mockConnection);
        assertTrue(sql, sql.contains("MOD(ABS(HASH(\"col1\")), 4) = 0"));
        assertTrue(sql, sql.contains("\"col2\""));
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

        Split split = bucketSplit(1, 4, "\"col1\"");

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

        Split split = bucketSplit(2, 4, "\"col1\"");

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
        // A pushed down LIMIT must not cost the split its scoping: it is a WHERE clause, so both apply.
        String sql = capturePreparedSql(mockConnection);
        assertTrue(sql, sql.contains("MOD(ABS(HASH(\"col1\")), 4) = 2"));
        assertTrue(sql, sql.contains("LIMIT 100"));
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

        Split split = bucketSplit(3, 4, "\"col1\"");

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

        Split split = bucketSplit(7, 50, "\"id\",\"name\"");

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
        String sql = capturePreparedSql(mockConnection);
        assertTrue(sql, sql.contains("MOD(ABS(HASH(\"id\",\"name\")), 50) = 7"));
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
        .add("partition", ALL_PARTITIONS) // All partitions
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
        // A single partition read has nothing to scope to, so no bucket predicate should appear.
        String sql = capturePreparedSql(mockConnection);
        assertTrue(sql, !sql.contains("HASH("));
    }

    @Test
    public void testGetPartitionWhereClausesWithNullSplit()
    {
        assertEquals(Collections.emptyList(), queryBuilder.getPartitionWhereClauses(null));
    }

    @Test
    public void testGetPartitionWhereClausesWithNoPartitionProperty()
    {
        // The S3 export and query passthrough paths both pass splits without a partition property.
        Split split = Split.newBuilder(
                S3SpillLocation.newBuilder().withBucket("test").withPrefix("test").build(),
                null
        ).build();

        assertEquals(Collections.emptyList(), queryBuilder.getPartitionWhereClauses(split));
    }

    @Test
    public void testGetPartitionWhereClausesWithAllPartitions()
    {
        assertEquals(Collections.emptyList(), queryBuilder.getPartitionWhereClauses(partitionSplit(ALL_PARTITIONS)));
    }

    @Test
    public void testGetPartitionWhereClausesWithSingleKey()
    {
        assertEquals(
                Collections.singletonList("MOD(ABS(HASH(\"id\")), 50) = 12"),
                queryBuilder.getPartitionWhereClauses(bucketSplit(12, 50, "\"id\"")));
    }

    @Test
    public void testGetPartitionWhereClausesWithCompositeKey()
    {
        assertEquals(
                Collections.singletonList("MOD(ABS(HASH(\"col1\",\"col2\")), 4) = 0"),
                queryBuilder.getPartitionWhereClauses(bucketSplit(0, 4, "\"col1\",\"col2\"")));
    }

    @Test
    public void testGetPartitionWhereClausesWithEmbeddedDashInKey()
    {
        // The key is the last field in the encoding precisely so that dashes in column names survive it.
        assertEquals(
                Collections.singletonList("MOD(ABS(HASH(\"my-key-col\")), 8) = 3"),
                queryBuilder.getPartitionWhereClauses(bucketSplit(3, 8, "\"my-key-col\"")));
    }

    @Test
    public void testGetPartitionWhereClausesWithEmbeddedQuoteInKey()
    {
        assertEquals(
                Collections.singletonList("MOD(ABS(HASH(\"we\"\"ird\")), 8) = 3"),
                queryBuilder.getPartitionWhereClauses(bucketSplit(3, 8, "\"we\"\"ird\"")));
    }

    @Test
    public void testGetPartitionWhereClausesRejectsUnrecognizedPartition()
    {
        // Returning no predicate would silently duplicate the table once per split, so this must fail loudly.
        assertThrows(AthenaConnectorException.class,
                () -> queryBuilder.getPartitionWhereClauses(partitionSplit("some-random-value")));
    }

    @Test
    public void testGetPartitionWhereClausesRejectsLegacyLimitOffsetPartition()
    {
        assertThrows(AthenaConnectorException.class,
                () -> queryBuilder.getPartitionWhereClauses(partitionSplit("partition-primary-\"id\"-limit-1000-offset-500")));
    }

    @Test
    public void testGetPartitionWhereClausesRejectsUnquotedKey()
    {
        assertThrows(AthenaConnectorException.class,
                () -> queryBuilder.getPartitionWhereClauses(bucketSplit(0, 4, "id")));
    }

    @Test
    public void testGetPartitionWhereClausesRejectsInjectedKey()
    {
        assertThrows(AthenaConnectorException.class,
                () -> queryBuilder.getPartitionWhereClauses(bucketSplit(0, 4, "\"id\") OR 1=1 OR MOD(ABS(HASH(\"id\"")));
    }

    @Test
    public void testGetSqlDialect()
    {
        org.apache.calcite.sql.SqlDialect dialect = queryBuilder.getSqlDialect();
        assertNotNull(dialect);
        assertTrue(dialect instanceof org.apache.calcite.sql.dialect.SnowflakeSqlDialect);
    }

    @Test
    public void testGetSqlDialectWithCasingFilter()
    {
        org.apache.calcite.sql.SqlDialect dialect = queryBuilder.getSqlDialect(true);
        assertNotNull(dialect);
        assertTrue(dialect instanceof SnowflakeDialect);
    }

    @Test
    public void testGetSqlDialectWithCasingFilterFalse()
    {
        org.apache.calcite.sql.SqlDialect dialect = queryBuilder.getSqlDialect(false);
        assertNotNull(dialect);
        assertTrue(dialect instanceof SnowflakeDialect);
    }
}
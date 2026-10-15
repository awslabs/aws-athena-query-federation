/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream.query;

import com.amazonaws.athena.connectors.timestream.TestUtils;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;

import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.VIEW_METADATA_FIELD;
import static org.junit.Assert.*;

public class SelectQueryBuilderTest
{
    private static final Logger logger = LoggerFactory.getLogger(SelectQueryBuilderTest.class);
    private static final String DATABASE_NAME = "myDatabase";
    private static final String TABLE_NAME = "myTable";
    private static final String COL_1 = "col1";
    private static final String VIEW_SOURCE_TABLE = "test_table";
    private static final String VIEW_DEFINITION = "SELECT " + COL_1 + " from " + VIEW_SOURCE_TABLE;
    private static final String COL_2 = "col2";
    private static final String COL_3 = "col3";
    private static final String COL_4 = "col4";
    private static final String VAL_1 = "val1";
    private static final String VAL_2 = "val2";
    private static final String TIME_COL_0 = "time0";
    private static final String TIME_COL_1 = "time1";
    private static final String VAL_COLUMN = "val";

    private QueryFactory queryFactory = new QueryFactory();
    private BlockAllocator allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        if (allocator != null) {
            allocator.close();
        }
    }

    /**
     * Normalizes a query string by removing all whitespace characters for comparison.
     */
    private String normalizeQuery(String query)
    {
        return query.replaceAll("\\s+", " ").trim();
    }

    @Test
    public void build_WithConstraintsAndSchema_ReturnsSelectQueryWithWherePredicates()
    {
        logger.info("build: enter");

        String expected = "SELECT \"col1\", \"col2\", \"col3\", \"col4\" FROM \"myDatabase\".\"myTable\" WHERE (\"col4\" IN ('val1','val2')) AND ((\"col2\" < 1)) AND (\"col3\" IN (20000,10000)) AND ((\"col1\" > 1))";

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(COL_1, SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false));
        constraintsMap.put(COL_2, SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.lessThan(allocator, Types.MinorType.INT.getType(), 1)), false));
        constraintsMap.put(COL_3, EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(), true, true)
                .add(20000L)
                .add(10000L)
                .build());
        constraintsMap.put(COL_4, EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add(VAL_1)
                .add(VAL_2)
                .build());

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField(COL_1)
                .addIntField(COL_2)
                .addBigIntField(COL_3)
                .addStringField(COL_4)
                .build();

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withProjection(schema)
                .withConjucts(TestUtils.constraints(constraintsMap, Collections.emptyMap()))
                .build();

        logger.info("build: actual[{}]", actual);
        assertEquals("Select query with constraints should match expected", normalizeQuery(expected), normalizeQuery(actual));

        logger.info("build: exit");
    }

    @Test
    public void build_WithTimeConstraints_IncludesTimeComparisonsInWhereClause() {
        logger.info("build_WithTimeConstraints_IncludesTimeComparisonsInWhereClause: enter");

        String expected = "SELECT \"val\" FROM \"myDatabase\".\"myTable\" WHERE ((\"time1\" > '2024-04-05 09:31:12.000000000')) AND ((\"time0\" > '2024-04-05 09:31:12.142000000'))";
        
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(TIME_COL_0, SortedRangeSet.copyOf(Types.MinorType.DATEMILLI.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.DATEMILLI.getType(),
                        LocalDateTime.of(2024, 4, 5, 9, 31, 12, 142000000))), false));
        constraintsMap.put(TIME_COL_1, SortedRangeSet.copyOf(Types.MinorType.DATEMILLI.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.DATEMILLI.getType(),
                        LocalDateTime.of(2024, 4, 5, 9, 31, 12))), false));

        Schema schema = SchemaBuilder.newBuilder()
                .addField(VAL_COLUMN, Types.MinorType.DATEMILLI.getType())
                .build();

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withProjection(schema)
                .withConjucts(TestUtils.constraints(constraintsMap, Collections.emptyMap()))
                .build();

        logger.info("build: actual[{}]", actual);
        assertEquals("Select query with time constraints should match expected", normalizeQuery(expected), normalizeQuery(actual));

        logger.info("build_WithTimeConstraints_IncludesTimeComparisonsInWhereClause: exit");
    }

    @Test
    public void build_WithViewMetadata_WrapsQueryInWithClauseAndSelectsFromAlias()
    {
        logger.info("build_WithViewMetadata_WrapsQueryInWithClauseAndSelectsFromAlias: enter");

        String expected = "WITH t1 AS ( SELECT col1 from test_table )  SELECT \"col1\", \"col2\", \"col3\", \"col4\" FROM t1 WHERE ((\"col2\" < 1)) AND ((\"col1\" > 1))";

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField(COL_1)
                .addIntField(COL_2)
                .addBigIntField(COL_3)
                .addStringField(COL_4)
                .addMetadata(VIEW_METADATA_FIELD, VIEW_DEFINITION)
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(COL_1, SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false));
        constraintsMap.put(COL_2, SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.lessThan(allocator, Types.MinorType.INT.getType(), 1)), false));

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withProjection(schema)
                .withConjucts(TestUtils.constraints(constraintsMap, Collections.emptyMap()))
                .build();

        logger.info("build_WithViewMetadata_WrapsQueryInWithClauseAndSelectsFromAlias: actual[{}]", actual);
        assertEquals("Select query with view metadata should match expected", normalizeQuery(expected), normalizeQuery(actual));

        logger.info("build_WithViewMetadata_WrapsQueryInWithClauseAndSelectsFromAlias: exit");
    }

    @Test(expected = NullPointerException.class)
    public void build_WithNullDatabaseName_ThrowsNullPointerException() {
        Schema schema = SchemaBuilder.newBuilder().addStringField(COL_1).build();
        queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName(null)
                .withTableName(TABLE_NAME)
                .withProjection(schema)
                .withConjucts(TestUtils.constraints(Collections.emptyMap(), Collections.emptyMap()))
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void build_WithNullTableName_ThrowsNullPointerException() {
        Schema schema = SchemaBuilder.newBuilder().addStringField(COL_1).build();
        queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName(DATABASE_NAME)
                .withTableName(null)
                .withProjection(schema)
                .withConjucts(TestUtils.constraints(Collections.emptyMap(), Collections.emptyMap()))
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void build_WithNullProjection_ThrowsNullPointerException() {
        queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withConjucts(TestUtils.constraints(Collections.emptyMap(), Collections.emptyMap()))
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void build_WithEmptyProjection_ThrowsIllegalArgumentException() {
        Schema emptySchema = SchemaBuilder.newBuilder().build();
        queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName(DATABASE_NAME)
                .withTableName(TABLE_NAME)
                .withProjection(emptySchema)
                .withConjucts(TestUtils.constraints(Collections.emptyMap(), Collections.emptyMap()))
                .build();
    }
}

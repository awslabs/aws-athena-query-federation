/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlServerSqlUtilsTest
{
    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE_NAME = "test_table";
    private static final TableName tableName = new TableName(SCHEMA_NAME, TABLE_NAME);
    
    private static final ArrowType INT_TYPE = new ArrowType.Int(32, false);
    private static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    
    private BlockAllocatorImpl allocator;
    private Split split;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void buildSql_WithBasicQuery_GeneratesSelectFromQuery()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        Schema schema = makeSchema(Collections.emptyMap());
        
        String expectedSql = "SELECT null FROM \"test_schema\".\"test_table\"";
        List<TypeAndValue> expectedParams = Collections.emptyList();
        
        Constraints constraints = getConstraints(constraintMap, Collections.emptyList());
        executeAndVerify(constraints, schema, expectedParams, expectedSql);
    }

    @Test
    public void buildSql_WithConstraintsRanges_GeneratesQueryWithWhereClause()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeSet);

        Schema schema = makeSchema(constraintMap);
        Constraints constraints = getConstraints(constraintMap, Collections.emptyList());
        
        List<TypeAndValue> expectedParams = new ArrayList<>();
        expectedParams.add(new TypeAndValue(INT_TYPE, 10));
        expectedParams.add(new TypeAndValue(INT_TYPE, 20));
        String expectedSql = "SELECT \"intCol\" FROM \"test_schema\".\"test_table\"  WHERE (\"intCol\" > ? AND \"intCol\" < ?)";
        
        executeAndVerify(constraints, schema, expectedParams, expectedSql);
    }

    @Test
    public void buildSql_WithInPredicate_GeneratesQueryWithInClause()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet inSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 20), Marker.exactly(allocator, INT_TYPE, 20)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 30), Marker.exactly(allocator, INT_TYPE, 30)))
                .build();
        constraintMap.put("intCol", inSet);

        Schema schema = makeSchema(constraintMap);
        Constraints constraints = getConstraints(constraintMap, Collections.emptyList());
        
        List<TypeAndValue> expectedParams = new ArrayList<>();
        expectedParams.add(new TypeAndValue(INT_TYPE, 10));
        expectedParams.add(new TypeAndValue(INT_TYPE, 20));
        expectedParams.add(new TypeAndValue(INT_TYPE, 30));
        String expectedSql = "SELECT \"intCol\" FROM \"test_schema\".\"test_table\"  WHERE (\"intCol\" IN (?,?,?))";
        
        executeAndVerify(constraints, schema, expectedParams, expectedSql);
    }

    @Test
    public void buildSql_WithOrderBy_GeneratesQueryWithOrderByWithoutLimit()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", rangeSet);

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField("intCol", OrderByField.Direction.ASC_NULLS_FIRST));
        orderByFields.add(new OrderByField("stringCol", OrderByField.Direction.DESC_NULLS_LAST));

        Schema schema = makeSchema(constraintMap);
        Constraints constraints = getConstraints(constraintMap, orderByFields);
        
        List<TypeAndValue> expectedParams = new ArrayList<>();
        expectedParams.add(new TypeAndValue(INT_TYPE, 10));
        String expectedSql = "SELECT \"intCol\" FROM \"test_schema\".\"test_table\"  WHERE (\"intCol\" = ?) ORDER BY \"intCol\" ASC NULLS FIRST, \"stringCol\" DESC NULLS LAST";
        
        executeAndVerify(constraints, schema, expectedParams, expectedSql);
    }

    @Test
    public void buildSql_WithNullPredicate_GeneratesQueryWithIsNull()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet nullSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();
        constraintMap.put("intCol", nullSet);

        Schema schema = makeSchema(constraintMap);
        Constraints constraints = getConstraints(constraintMap, Collections.emptyList());
        
        List<TypeAndValue> expectedParams = Collections.emptyList();
        String expectedSql = "SELECT \"intCol\" FROM \"test_schema\".\"test_table\"  WHERE \"intCol\" IS NULL";
        
        executeAndVerify(constraints, schema, expectedParams, expectedSql);
    }

    @Test
    public void buildSql_WithNotNullPredicate_GeneratesQueryWithIsNotNull()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet notNullSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintMap.put("intCol", notNullSet);

        Schema schema = makeSchema(constraintMap);
        Constraints constraints = getConstraints(constraintMap, Collections.emptyList());
        
        List<TypeAndValue> expectedParams = Collections.emptyList();
        String expectedSql = "SELECT \"intCol\" FROM \"test_schema\".\"test_table\"  WHERE \"intCol\" IS NOT NULL";
        
        executeAndVerify(constraints, schema, expectedParams, expectedSql);
    }

    @Test
    public void buildSql_WithPartitionClause_GeneratesQueryWithPartitionFunction()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        Schema schema = makeSchema(constraintMap);
        Constraints constraints = getConstraints(constraintMap, Collections.emptyList());

        // Create split with partition information
        Map<String, String> splitProperties = new LinkedHashMap<>();
        splitProperties.put("PARTITION_FUNCTION", "pf");
        splitProperties.put("PARTITIONING_COLUMN", "testCol1");
        splitProperties.put("partition_number", "1");
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);
        when(splitWithPartition.getProperty("PARTITION_FUNCTION")).thenReturn("pf");
        when(splitWithPartition.getProperty("PARTITIONING_COLUMN")).thenReturn("testCol1");
        when(splitWithPartition.getProperty("partition_number")).thenReturn("1");

        String expectedSql = "SELECT null FROM \"test_schema\".\"test_table\"  WHERE  $PARTITION.pf(testCol1) = 1";

        List<TypeAndValue> parameterValues = new ArrayList<>();
        String sql = SqlServerSqlUtils.buildSql(tableName, schema, constraints, splitWithPartition, parameterValues);

        assertEquals("SQL should match expected", expectedSql, sql);
        assertEquals("Parameter count should match", 0, parameterValues.size());
    }

    @Test
    public void buildSql_WithMultipleColumns_GeneratesQueryWithAllColumns()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet intSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        ValueSet stringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, "test"), Marker.exactly(allocator, STRING_TYPE, "test")))
                .build();
        constraintMap.put("intCol", intSet);
        constraintMap.put("stringCol", stringSet);

        Schema schema = makeSchema(constraintMap);
        Constraints constraints = getConstraints(constraintMap, Collections.emptyList());
        
        List<TypeAndValue> expectedParams = new ArrayList<>();
        expectedParams.add(new TypeAndValue(INT_TYPE, 10));
        expectedParams.add(new TypeAndValue(STRING_TYPE, "test"));
        String expectedSql = "SELECT \"intCol\", \"stringCol\" FROM \"test_schema\".\"test_table\"  WHERE (\"intCol\" = ?) AND (\"stringCol\" = ?)";
        
        executeAndVerify(constraints, schema, expectedParams, expectedSql);
    }

    @Test
    public void buildSql_WithEmptySchema_GeneratesQueryWithNull()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        Schema emptySchema = new Schema(Collections.emptyList());
        Constraints constraints = getConstraints(constraintMap, Collections.emptyList());

        String expectedSql = "SELECT null FROM \"test_schema\".\"test_table\"";
        
        List<TypeAndValue> parameterValues = new ArrayList<>();
        String sql = SqlServerSqlUtils.buildSql(tableName, emptySchema, constraints, split, parameterValues);
        
        assertEquals("SQL should match expected", expectedSql, sql);
        assertEquals("Parameter count should match", 0, parameterValues.size());
    }

    private Schema makeSchema(Map<String, ValueSet> constraintMap)
    {
        List<Field> fields = new ArrayList<>();
        for (String columnName : constraintMap.keySet()) {
            ArrowType type = constraintMap.get(columnName).getType();
            fields.add(Field.nullable(columnName, type));
        }
        return new Schema(fields);
    }

    private Constraints getConstraints(Map<String, ValueSet> constraintMap, List<OrderByField> orderByFields)
    {
        return new Constraints(constraintMap, Collections.emptyList(), orderByFields, Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }

    private void executeAndVerify(Constraints constraints, Schema schema, List<TypeAndValue> expectedParams, String expectedSql)
    {
        List<TypeAndValue> parameterValues = new ArrayList<>();
        String sql = SqlServerSqlUtils.buildSql(tableName, schema, constraints, split, parameterValues);
        
        assertEquals("SQL should match expected", expectedSql, sql);
        assertEquals("Parameter count should match", expectedParams.size(), parameterValues.size());
    }
}


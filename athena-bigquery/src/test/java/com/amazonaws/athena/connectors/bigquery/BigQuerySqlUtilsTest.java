/*-
 * #%L
 * athena-bigquery
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

package com.amazonaws.athena.connectors.bigquery;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BigQuerySqlUtilsTest
{
    static final TableName tableName = new TableName("schema", "table");
    static final Split split = null;

    static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;
    static final ArrowType INT_TYPE = new ArrowType.Int(32, true);

    @Test
    public void testSqlWithConstraintsEquality()
        throws Exception
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        constraintMap.put("bool1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), BOOLEAN_TYPE,
            true, false).add(false).build());
        constraintMap.put("int1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), INT_TYPE,
            true, false).add(14).build());
        constraintMap.put("nullableField", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), INT_TYPE,
            true, true).build());

        try (Constraints constraints = new Constraints(constraintMap)) {
            String sql = BigQuerySqlUtils.buildSqlFromSplit(tableName, makeSchema(constraintMap), constraints, split);
            assertEquals("SELECT bool1,int1,nullableField from schema.table WHERE (bool1 = false) AND (int1 = 14) AND (nullableField is null)", sql);
        }
    }

    @Test
    public void testSqlWithConstraintsRanges()
        throws Exception
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, true).add(new Range(Marker.above(new BlockAllocatorImpl(), INT_TYPE, 10),
            Marker.exactly(new BlockAllocatorImpl(), INT_TYPE, 20))).build();

        ValueSet isNullRangeSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();

        ValueSet isNonNullRangeSet = SortedRangeSet.newBuilder(INT_TYPE, false).add(
            new Range(Marker.lowerUnbounded(new BlockAllocatorImpl(), INT_TYPE),
                      Marker.upperUnbounded(new BlockAllocatorImpl(), INT_TYPE)))
            .build();

        constraintMap.put("integerRange", rangeSet);
        constraintMap.put("isNullRange", isNullRangeSet);
        constraintMap.put("isNotNullRange", isNonNullRangeSet);

        try (Constraints constraints = new Constraints(constraintMap)) {
            String sql = BigQuerySqlUtils.buildSqlFromSplit(tableName, makeSchema(constraintMap), constraints, split);
            assertEquals("SELECT integerRange,isNullRange,isNotNullRange from schema.table WHERE (integerRange > 10) AND (integerRange <= 20) AND (isNullRange is null) AND (isNotNullRange is not null)", sql);
        }
    }

    private Schema makeSchema(Map<String, ValueSet> constraintMap)
    {
        SchemaBuilder builder = new SchemaBuilder();
        for (Map.Entry<String, ValueSet> field : constraintMap.entrySet()) {
            ArrowType.ArrowTypeID typeId = field.getValue().getType().getTypeID();
            switch (typeId) {
                case Int:
                    builder.addIntField(field.getKey());
                    break;
                case Bool:
                    builder.addBitField(field.getKey());
                    break;
                case Utf8:
                    builder.addStringField(field.getKey());
                default:
                    throw new UnsupportedOperationException("Type Not Implemented: " + typeId.name());
            }
        }
        return builder.build();
    }
}

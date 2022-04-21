/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BigQuerySqlUtilsTest
{
    static final TableName tableName = new TableName("schema", "table");
    static final Split split = Mockito.mock(Split.class);

    static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;
    static final ArrowType INT_TYPE = new ArrowType.Int(32, true);
    static final ArrowType STRING_TYPE = new ArrowType.Utf8();

    @Test
    public void testSqlWithConstraintsRanges()
            throws Exception
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, true).add(new Range(Marker.above(new BlockAllocatorImpl(), INT_TYPE, 10),
                Marker.exactly(new BlockAllocatorImpl(), INT_TYPE, 20))).build();

        ValueSet isNullRangeSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();

        ValueSet isNonNullRangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(new BlockAllocatorImpl(), INT_TYPE), Marker.upperUnbounded(new BlockAllocatorImpl(), INT_TYPE)))
                .build();

        ValueSet stringRangeSet = SortedRangeSet.newBuilder(STRING_TYPE, false).add(new Range(Marker.exactly(new BlockAllocatorImpl(), STRING_TYPE, "a_low"),
                Marker.below(new BlockAllocatorImpl(), STRING_TYPE, "z_high"))).build();

        ValueSet booleanRangeSet = SortedRangeSet.newBuilder(BOOLEAN_TYPE, false).add(new Range(Marker.exactly(new BlockAllocatorImpl(), BOOLEAN_TYPE, true),
                Marker.exactly(new BlockAllocatorImpl(), BOOLEAN_TYPE, true))).build();

        ValueSet integerInRangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(new BlockAllocatorImpl(), INT_TYPE, 10), Marker.exactly(new BlockAllocatorImpl(), INT_TYPE, 10)))
                .add(new Range(Marker.exactly(new BlockAllocatorImpl(), INT_TYPE, 1000_000), Marker.exactly(new BlockAllocatorImpl(), INT_TYPE, 1000_000)))
                .build();

        constraintMap.put("integerRange", rangeSet);
        constraintMap.put("isNullRange", isNullRangeSet);
        constraintMap.put("isNotNullRange", isNonNullRangeSet);
        constraintMap.put("stringRange", stringRangeSet);
        constraintMap.put("booleanRange", booleanRangeSet);
        constraintMap.put("integerInRange", integerInRangeSet);

        Mockito.when(split.getProperties()).thenReturn(Collections.emptyMap());

        final List<QueryParameterValue> expectedParameterValues = ImmutableList.of(QueryParameterValue.int64(10), QueryParameterValue.int64(20),
                QueryParameterValue.string("a_low"), QueryParameterValue.string("z_high"),
                QueryParameterValue.bool(true),
                QueryParameterValue.int64(10), QueryParameterValue.int64(1000000));

        try (Constraints constraints = new Constraints(constraintMap)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSqlFromSplit(tableName, makeSchema(constraintMap), constraints, split, parameterValues);
            assertEquals(expectedParameterValues, parameterValues);
            assertEquals("SELECT `integerRange`,`isNullRange`,`isNotNullRange`,`stringRange`,`booleanRange`,`integerInRange` from `schema`.`table` " +
                    "WHERE ((integerRange IS NULL) OR (`integerRange` > ? AND `integerRange` <= ?)) " +
                    "AND (isNullRange IS NULL) AND (isNotNullRange IS NOT NULL) " +
                    "AND ((`stringRange` >= ? AND `stringRange` < ?)) " +
                    "AND (`booleanRange` = ?) " +
                    "AND (`integerInRange` IN (?,?))", sql);
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
                    break;
                default:
                    throw new UnsupportedOperationException("Type Not Implemented: " + typeId.name());
            }
        }
        return builder.build();
    }
}

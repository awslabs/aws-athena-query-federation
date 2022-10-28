/*-
 * #%L
 * athena-msk
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
package com.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
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

public class AmazonMskSqlUtilsTest {

    @Test
    public void testSqlWithConstraintsRanges()
    {
        TableName tableName = new TableName("schema", "table");
        ArrowType booleanType = ArrowType.Bool.INSTANCE;
        ArrowType intType = new ArrowType.Int(32, true);
        ArrowType stringType = new ArrowType.Utf8();
        Split split = Mockito.mock(Split.class);
        List<String> parameterValues = new ArrayList<>();

        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(intType, true).add(new Range(Marker.above(new BlockAllocatorImpl(), intType, 10),
                Marker.exactly(new BlockAllocatorImpl(), intType, 20))).build();

        ValueSet isNullRangeSet = SortedRangeSet.newBuilder(intType, true).build();

        ValueSet isNonNullRangeSet = SortedRangeSet.newBuilder(intType, false)
                .add(new Range(Marker.lowerUnbounded(new BlockAllocatorImpl(), intType), Marker.upperUnbounded(new BlockAllocatorImpl(), intType)))
                .build();

        ValueSet stringRangeSet = SortedRangeSet.newBuilder(stringType, false).add(new Range(Marker.exactly(new BlockAllocatorImpl(), stringType, "a_low"),
                Marker.below(new BlockAllocatorImpl(), stringType, "z_high"))).build();

        ValueSet booleanRangeSet = SortedRangeSet.newBuilder(booleanType, false).add(new Range(Marker.exactly(new BlockAllocatorImpl(), booleanType, true),
                Marker.exactly(new BlockAllocatorImpl(), booleanType, true))).build();

        ValueSet integerInRangeSet = SortedRangeSet.newBuilder(intType, false)
                .add(new Range(Marker.exactly(new BlockAllocatorImpl(), intType, 10), Marker.exactly(new BlockAllocatorImpl(), intType, 10)))
                .add(new Range(Marker.exactly(new BlockAllocatorImpl(), intType, 1000_000), Marker.exactly(new BlockAllocatorImpl(), intType, 1000_000)))
                .build();

        constraintMap.put("integerRange", rangeSet);
        constraintMap.put("isNullRange", isNullRangeSet);
        constraintMap.put("isNotNullRange", isNonNullRangeSet);
        constraintMap.put("stringRange", stringRangeSet);
        constraintMap.put("booleanRange", booleanRangeSet);
        constraintMap.put("integerInRange", integerInRangeSet);

        Constraints constraints = new Constraints(constraintMap);

        Mockito.when(split.getProperties()).thenReturn(Collections.emptyMap());

        String sql = AmazonMskSqlUtils.buildSqlFromSplit(tableName, makeSchema(constraintMap), constraints, split, parameterValues);
        assertEquals("SELECT integerRange,isNullRange,isNotNullRange,stringRange,booleanRange,integerInRange from schema.table " +
                "WHERE ((integerRange IS NULL) OR (integerRange >10 AND integerRange <=20)) " +
                "AND (isNullRange IS NULL) AND (isNotNullRange IS NOT NULL) " +
                "AND ((stringRange >='a_low' AND stringRange <'z_high')) " +
                "AND (booleanRange =true) " +
                "AND (integerInRange IN (?,?))", sql);

        sql = AmazonMskSqlUtils.buildSqlFromSplit(tableName, makeSchema(new LinkedHashMap<>()), constraints, split, parameterValues);
        assertEquals("SELECT null from schema.table", sql);
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

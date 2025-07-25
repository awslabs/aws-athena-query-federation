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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryTestUtils.makeSchema;
import static org.junit.Assert.assertEquals;

public class BigQuerySqlUtilsTest
{
    static final TableName tableName = new TableName("schema", "table");
    static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;
    static final ArrowType INT_TYPE = new ArrowType.Int(32, false);
    static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    private BlockAllocatorImpl allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testSqlWithConstraintsRanges()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, true).add(new Range(Marker.above(allocator, INT_TYPE, 10),
                Marker.below(allocator, INT_TYPE, 20))).build();

        ValueSet isNullRangeSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();

        ValueSet isNonNullRangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();

        ValueSet stringRangeSet = SortedRangeSet.newBuilder(STRING_TYPE, false).add(new Range(Marker.exactly(allocator, STRING_TYPE, "a_low"),
                Marker.below(allocator, STRING_TYPE, "z_high"))).build();

        ValueSet booleanRangeSet = SortedRangeSet.newBuilder(BOOLEAN_TYPE, false).add(new Range(Marker.exactly(allocator, BOOLEAN_TYPE, true),
                Marker.exactly(allocator, BOOLEAN_TYPE, true))).build();

        ValueSet integerInRangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 1000_000), Marker.exactly(allocator, INT_TYPE, 1000_000)))
                .build();

        constraintMap.put("integerRange", rangeSet);
        constraintMap.put("isNullRange", isNullRangeSet);
        constraintMap.put("isNotNullRange", isNonNullRangeSet);
        constraintMap.put("stringRange", stringRangeSet);
        constraintMap.put("booleanRange", booleanRangeSet);
        constraintMap.put("integerInRange", integerInRangeSet);

        final List<QueryParameterValue> expectedParameterValues = ImmutableList.of(QueryParameterValue.int64(10), QueryParameterValue.int64(20),
                QueryParameterValue.string("a_low"), QueryParameterValue.string("z_high"),
                QueryParameterValue.bool(true),
                QueryParameterValue.int64(10), QueryParameterValue.int64(1000000));

        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            assertEquals(expectedParameterValues, parameterValues);
            assertEquals("SELECT `integerRange`,`isNullRange`,`isNotNullRange`,`stringRange`,`booleanRange`,`integerInRange` from `schema`.`table` " +
                    "WHERE ((integerRange IS NULL) OR (`integerRange` > ? AND `integerRange` < ?)) " +
                    "AND (isNullRange IS NULL) AND (isNotNullRange IS NOT NULL) " +
                    "AND ((`stringRange` >= ? AND `stringRange` < ?)) " +
                    "AND (`booleanRange` = ?) " +
                    "AND (`integerInRange` IN (?,?))", sql);
        }
    }
}

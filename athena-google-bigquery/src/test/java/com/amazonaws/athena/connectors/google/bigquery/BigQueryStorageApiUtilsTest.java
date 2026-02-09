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
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryTestUtils.makeSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BigQueryStorageApiUtilsTest
{
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
    public void testSetConstraints()
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
            List<String> fields = new ArrayList<>();
            for (Field field : makeSchema(constraintMap).getFields()) {
                fields.add(field.getName());
            }

            ReadSession.TableReadOptions.Builder optionsBuilder =
                    ReadSession.TableReadOptions.newBuilder()
                            .addAllSelectedFields(fields);
            ReadSession.TableReadOptions.Builder option = BigQueryStorageApiUtils.setConstraints(optionsBuilder, makeSchema(constraintMap), constraints);

            assertEquals("selected_fields: \"integerRange\"\n" +
                    "selected_fields: \"isNullRange\"\n" +
                    "selected_fields: \"isNotNullRange\"\n" +
                    "selected_fields: \"stringRange\"\n" +
                    "selected_fields: \"booleanRange\"\n" +
                    "selected_fields: \"integerInRange\"\n" +
                    "row_restriction: \"integerRange IS NULL OR integerRange > 10 AND integerRange < 20 AND isNullRange IS NULL AND isNotNullRange IS NOT NULL AND stringRange >= \\\"a_low\\\" AND stringRange < \\\"z_high\\\" AND booleanRange = true AND integerInRange IN (10,1000000)\"\n", option.toString());
        }
    }

    @Test
    public void testSetConstraints_SubstraitIN()
    {
        // SQL: "SELECT id FROM \"my_dataset\".\"service_requests_no_noise\" WHERE (id = 100 OR id = 200) AND is_active != true LIMIT 10";
        String substraitPlanString = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIQGg4IARABGghhbmQ6Ym9vbBIPGg0IARACGgdvcjpib29sEhUaEwgCEAMaDWVxdWFsOmFueV9hbnkSGRoXCAIQBBoRbm90X2VxdWFsOmFueV9hbnkaiAYShQYK/gUa+wUKAgoAEvAFOu0FCgUSAwoBFhLXBRLUBQoCCgASlwQKlAQKAgoAEuQDCglpc19hY3RpdmUKC3RpbnlpbnRfY29sCgxzbWFsbGludF9jb2wKCHByaW9yaXR5CgpiaWdpbnRfY29sCglmbG9hdF9jb2wKCmRvdWJsZV9jb2wKCHJlYWxfY29sCgt2YXJjaGFyX2NvbAoIY2hhcl9jb2wKDXZhcmJpbmFyeV9jb2wKCGRhdGVfY29sCgh0aW1lX2NvbAoNdGltZXN0YW1wX2NvbAoCaWQKDGRlY2ltYWxfY29sMgoMZGVjaW1hbF9jb2wzCgtzdWJjYXRlZ29yeQoNaW50X2FycmF5X2NvbAoHbWFwX2NvbAoQbWFwX3dpdGhfZGVjaW1hbAoMbmVzdGVkX2FycmF5EtYBCgQKAhACCgQSAhACCgQaAhACCgQqAhACCgQ6AhACCgRaAhACCgRaAhACCgRSAhACCgRiAhACCgeqAQQIARgCCgRqAhACCgWCAQIQAgoFigECEAIKBYoCAhgCCgnCAQYIBBATIAIKCcIBBggCEAogAgoJwgEGCAoQEyACCgvaAQgKBGICEAIYAgoL2gEICgQqAhACGAIKEeIBDgoEYgIQAhIEKgIQAiACChbiARMKBGICEAISCcIBBggCEAogAiACChLaAQ8KC9oBCAoEKgIQAhgCGAIYAjonCgpteV9kYXRhc2V0ChlzZXJ2aWNlX3JlcXVlc3RzX25vX25vaXNlGrMBGrABCAEaBAoCEAIigwEagAEafggCGgQKAhACIjkaNxo1CAMaBAoCEAIiDBoKEggKBBICCA4iACIdGhsKGcIBFgoQQEIPAAAAAAAAAAAAAAAAABATGAQiORo3GjUIAxoECgIQAiIMGgoSCAoEEgIIDiIAIh0aGwoZwgEWChCAhB4AAAAAAAAAAAAAAAAAEBMYBCIgGh4aHAgEGgQKAhACIgoaCBIGCgISACIAIgYaBAoCCAEaChIICgQSAggOIgAYACAKEgJJRDILEEoqB2lzdGhtdXM=";
        Schema testSchema = new Schema(Arrays.asList(
                new Field("employee_name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("job_title", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("address", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("salary", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("bonus", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("is_active", FieldType.nullable(new ArrowType.Bool()), null),
                new Field("join_date", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null)
        ));

        QueryPlan queryPlan = new QueryPlan("1.0", substraitPlanString);
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), queryPlan);

        ReadSession.TableReadOptions.Builder optionsBuilder = ReadSession.TableReadOptions.newBuilder();
        ReadSession.TableReadOptions.Builder result = BigQueryStorageApiUtils.setConstraints(optionsBuilder, testSchema, constraints);

        String rowRestriction = result.build().getRowRestriction();
        assertTrue("Row restriction mismatch",
                rowRestriction.contains("id IN (100.0000, 200.0000) AND NOT is_active"));
    }
}

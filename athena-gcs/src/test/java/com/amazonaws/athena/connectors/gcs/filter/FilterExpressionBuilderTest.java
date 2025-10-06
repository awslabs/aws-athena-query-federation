/*-
 * #%L
 * Amazon Athena GCS Connector
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
package com.amazonaws.athena.connectors.gcs.filter;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.GcsTestUtils;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.glue.model.Column;

import java.util.Collections;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilterExpressionBuilderTest
{
    @Test
    public void testGetExpressions()
    {
        Map<String, java.util.Optional<java.util.Set<String>>> result = FilterExpressionBuilder.getConstraintsForPartitionedColumns(
            com.google.common.collect.ImmutableList.of(Column.builder().name("year").build()),
                new Constraints(GcsTestUtils.createSummaryWithLValueRangeEqual("year", new ArrowType.Utf8(), "1"),
                        Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));
        assertEquals(result.size(), 1);
        assertEquals(result.get("year").get(), com.google.common.collect.ImmutableSet.of("1"));
        assertEquals(result.get("yeAr").get(), com.google.common.collect.ImmutableSet.of("1"));
    }


}

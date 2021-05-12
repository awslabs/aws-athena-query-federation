package com.amazonaws.athena.connector.lambda.serde;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ConstraintSerializationTest
{
    private static final Logger logger = LoggerFactory.getLogger(ConstraintSerializationTest.class);

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
    public void serializationTest()
            throws Exception
    {
        logger.info("serializationTest - enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col2", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.BIGINT.getType(), 950L)), false));

        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.BIT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIT.getType(), false)), false));

        constraintsMap.put("col4", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), 950.0D)), false));

        constraintsMap.put("col5", SortedRangeSet.copyOf(Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "8"),
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "9")), false));

        try (
                GetTableLayoutRequest req = new GetTableLayoutRequest(IdentityUtil.fakeIdentity(),
                        "queryId",
                        "default",
                        new TableName("schema1", "table1"),
                        new Constraints(constraintsMap),
                        SchemaBuilder.newBuilder().build(),
                        new HashSet<>())
        ) {
            ObjectMapperUtil.assertSerialization(req);
        }

        logger.info("serializationTest - exit");
    }
}

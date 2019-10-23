package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
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
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.BIGINT.getType(), 950L))));

        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.BIT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIT.getType(), false))));

        constraintsMap.put("col4", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), 950.0D))));

        constraintsMap.put("col5", SortedRangeSet.copyOf(Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "8"),
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "9"))));

        try (
                GetTableLayoutRequest req = new GetTableLayoutRequest(IdentityUtil.fakeIdentity(),
                        "queryId",
                        "default",
                        new TableName("schema1", "table1"),
                        new Constraints(constraintsMap),
                        new HashMap<>())
        ) {
            ObjectMapperUtil.assertSerialization(req, req.getClass());
        }

        logger.info("serializationTest - exit");
    }
}

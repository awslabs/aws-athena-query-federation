package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.Types;

public class TestUtils
{
    private TestUtils() {}

    public static ValueSet makeStringEquals(BlockAllocator allocator, String value)
    {
        return EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true).add(value).build();
    }
}

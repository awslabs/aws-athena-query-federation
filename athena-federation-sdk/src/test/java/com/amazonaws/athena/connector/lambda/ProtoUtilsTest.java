/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.protobuf.util.JsonFormat;

public class ProtoUtilsTest {

    private BlockAllocator blockAllocator;

    @Before
    public void setup()
    {
        blockAllocator = new BlockAllocatorImpl();
    }

    @After
    public void cleanup()
    {
        // blockAllocator.close();
    }

    @Test
    public void testToAndFromProtoBlockWithRecords() throws Exception
    {
 
        Block block = BlockUtils.newBlock(blockAllocator, "col1", Types.MinorType.INT.getType(), List.of(1, 2, 3));
        com.amazonaws.athena.connector.lambda.proto.data.Block protoBlock = ProtoUtils.toProtoBlock(block);
        Block rewritten = ProtoUtils.fromProtoBlock(blockAllocator, protoBlock);
 
        // the block equals method doesn't test allocator id
        assertEquals(block.getAllocatorId(), rewritten.getAllocatorId());
        assertEquals(block, rewritten);
    }
    
    @Test
    public void testToAndFromProtoBlockEmpty() throws Exception
    {
        Block empty = BlockUtils.newEmptyBlock(blockAllocator, "column", Types.MinorType.INT.getType());
        com.amazonaws.athena.connector.lambda.proto.data.Block protoBlock = ProtoUtils.toProtoBlock(empty);
        Block rewritten = ProtoUtils.fromProtoBlock(blockAllocator, protoBlock);

        assertEquals(empty.getAllocatorId(), rewritten.getAllocatorId());
        assertEquals(empty, rewritten);
    }

    @Test
    public void testMarker() throws Exception
    {
        Marker marker = new Marker(
            BlockUtils.newBlock(blockAllocator, "col1", Types.MinorType.INT.getType(), List.of(1L)),
            Marker.Bound.ABOVE,
            false
        );
        assertEquals(marker, ProtoUtils.fromProtoMarker(blockAllocator, ProtoUtils.toProtoMarker(marker)));

        marker = new Marker(
            BlockUtils.newBlock(blockAllocator, "col1", Types.MinorType.VARCHAR.getType(), List.of("a")),
            Marker.Bound.EXACTLY,
            true);
        assertEquals(marker, ProtoUtils.fromProtoMarker(blockAllocator, ProtoUtils.toProtoMarker(marker)));

        marker = Marker.lowerUnbounded(blockAllocator, Types.MinorType.INT.getType());
        com.amazonaws.athena.connector.lambda.proto.domain.predicate.Marker protoMarker = ProtoUtils.toProtoMarker(marker);
         assertEquals(marker, ProtoUtils.fromProtoMarker(blockAllocator, protoMarker));
    }

    @Test
    public void testRange() throws Exception
    {
        Range between = Range.range(blockAllocator, Types.MinorType.INT.getType(), 10L, true, 15L, false);
        Range gtFive = Range.greaterThan(blockAllocator, Types.MinorType.INT.getType(), 5);

        List<Range> ranges = List.of(gtFive, between);
        List<com.amazonaws.athena.connector.lambda.proto.domain.predicate.Range> protoRanges = ProtoUtils.toProtoRanges(ranges);
        List<Range> backToRanges = ProtoUtils.fromProtoRanges(blockAllocator, protoRanges);
        assertEquals(between, backToRanges.get(1));
        assertEquals(gtFive, backToRanges.get(0));
    }

    @Test
    public void testAllOrNoneValueSet() throws Exception
    {
        ValueSet valueSet = new AllOrNoneValueSet(
            Types.MinorType.FLOAT8.getType(),
            true, // warning - isAll() in AllOrNoneValueSet() only returns true if all && nullAllowed
            true
        );

        com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet protoValueSet = ProtoUtils.toProtoValueSet(valueSet);
        ValueSet back = ProtoUtils.fromProtoValueSet(blockAllocator, protoValueSet);

        assertEquals(valueSet, back);
    }

    @Test
    public void testEquatableValueSet() throws Exception
    {
        ValueSet valueSet = new EquatableValueSet(
            BlockUtils.newBlock(blockAllocator, "col1", Types.MinorType.FLOAT8.getType(), List.of(1.0)),
            true,
            false
        );

        com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet protoValueSet = ProtoUtils.toProtoValueSet(valueSet);
        ValueSet back = ProtoUtils.fromProtoValueSet(blockAllocator, protoValueSet);

        assertEquals(valueSet, back);
    }

    @Test
    public void testSortedRangeSet() throws Exception
    {
        ValueSet valueSet = SortedRangeSet.copyOf(
            Types.MinorType.FLOAT8.getType(),
            List.of(Range.range(blockAllocator, Types.MinorType.FLOAT8.getType(), 10.0, true, 15.0, false), Range.lessThan(blockAllocator, Types.MinorType.FLOAT8.getType(), 25.0)),
            true
        );

        com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet protoValueSet = ProtoUtils.toProtoValueSet(valueSet);
        ValueSet back = ProtoUtils.fromProtoValueSet(blockAllocator, protoValueSet);

        assertEquals(valueSet, back);
    }

    @Test
    public void testSummaryMap() throws Exception
    {
        Map<String, ValueSet> summaryMap = Map.of(
            "col3", SortedRangeSet.copyOf(
                Types.MinorType.FLOAT8.getType(),
                List.of(Range.range(blockAllocator, Types.MinorType.FLOAT8.getType(), 10.0, true, 15.0, false), Range.lessThan(blockAllocator, Types.MinorType.FLOAT8.getType(), 25.0)),
                true
            ),
            "col5", new EquatableValueSet(
                BlockUtils.newBlock(blockAllocator, "col1", Types.MinorType.FLOAT8.getType(), List.of(1.0)),
                true,
                false
            ),
            "col9", new AllOrNoneValueSet(
                Types.MinorType.FLOAT8.getType(),
                false,
                true// false - false fails currently. it translates wrong.
            )
        );
        Map<String, com.amazonaws.athena.connector.lambda.proto.domain.predicate.ValueSet> protoSummaryMap = ProtoUtils.toProtoSummary(summaryMap);
        Map<String, ValueSet> back = ProtoUtils.fromProtoSummary(blockAllocator, protoSummaryMap);
        
        summaryMap.entrySet().forEach(e -> assertTrue(back.containsKey(e.getKey()) && back.get(e.getKey()).equals(e.getValue())));

        Constraints constraints = new Constraints(summaryMap);
        com.amazonaws.athena.connector.lambda.proto.domain.predicate.Constraints protoConstraints = ProtoUtils.toProtoConstraints(constraints);
        Constraints backConstraints = ProtoUtils.fromProtoConstraints(blockAllocator, protoConstraints);
        assertEquals(constraints, backConstraints);

    }
}

package com.amazonaws.athena.connector.lambda.domain.predicate;

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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.setValue;

/**
 * Constraints require typed values in the form of Markers. Each Marker contains, at most, one typed value. This model
 * is awkward to map into Apache Arrow's VectorSchema or RecordBatch constructs without significant memory overhead. To
 * reduce the memory requirement associated with Markers and constraint processing we use a MarkerFactory that is capable
 * of sharing an underlying Apache Arrow Block across multiple Markers. In Testing this was 100x more performant than
 * having a 1-1 relationship between Markers and VectorSchema.
 */
public class MarkerFactory
        implements AutoCloseable
{
    private final BlockAllocator allocator;
    //For each supported Apache Arrow Type we maintain a Block for its values.
    private final Map<ArrowType, Block> sharedMarkerBlocks = new HashMap<>();
    //For each shared Block in the above map, we maintain the 'next' available row.
    private final Map<ArrowType, Integer> markerLeases = new HashMap<>();

    /**
     * Creates a new MarkerFactory using the provided BlockAllocator.
     *
     * @param allocator The BlockAllocator to use when creating Apache Arrow resources.
     */
    public MarkerFactory(BlockAllocator allocator)
    {
        this.allocator = allocator;
    }

    /**
     * Creates a nullable Marker.
     *
     * @param type The type of the value represented by this Marker.
     * @param value The value of this Marker.
     * @param bound The Bound of this Marker.
     * @return The newly created SharedBlockMarker satisfying the supplied criteria.
     * @note SharedBlockMarker allow for sharing a single Apache Arrow Block across multiple Markers to reduce memory overhead.
     */
    public Marker createNullable(ArrowType type, Object value, Marker.Bound bound)
    {
        BlockLease lease = getOrCreateBlock(type);
        if (value != null) {
            setValue(lease.getBlock().getFieldVector(Marker.DEFAULT_COLUMN), lease.getPos(), value);
        }
        return new SharedBlockMarker(this, lease.getBlock(), lease.getPos(), bound, value == null);
    }

    /**
     * Creates a Marker without nulls.
     *
     * @param type The type of the value represented by this Marker.
     * @param value The value of this Marker.
     * @param bound The Bound of this Marker.
     * @return The newly created Marker satisfying the supplied criteria.
     * @note SharedBlockMarker allow for sharing a single Apache Arrow Block across multiple Markers to reduce memory overhead.
     */
    public Marker create(ArrowType type, Object value, Marker.Bound bound)
    {
        BlockLease lease = getOrCreateBlock(type);
        setValue(lease.getBlock().getFieldVector(Marker.DEFAULT_COLUMN), lease.getPos(), value);
        return new SharedBlockMarker(this, lease.getBlock(), lease.getPos(), bound, false);
    }

    /**
     * Creates an empty Marker without nulls.
     *
     * @param type The type of the value represented by this Marker.
     * @param bound The Bound of this Marker.
     * @return The newly created Marker satisfying the supplied criteria.
     * @note SharedBlockMarker allow for sharing a single Apache Arrow Block across multiple Markers to reduce memory overhead.
     */
    public Marker create(ArrowType type, Marker.Bound bound)
    {
        BlockLease lease = getOrCreateBlock(type);
        return new SharedBlockMarker(this, lease.getBlock(), lease.getPos(), bound, true);
    }

    private synchronized BlockLease getOrCreateBlock(ArrowType type)
    {
        Block sharedBlock = sharedMarkerBlocks.get(type);
        Integer leaseNumber = markerLeases.get(type);
        if (sharedBlock == null) {
            sharedBlock = BlockUtils.newEmptyBlock(allocator, Marker.DEFAULT_COLUMN, type);
            sharedMarkerBlocks.put(type, sharedBlock);
            leaseNumber = 0;
        }
        markerLeases.put(type, ++leaseNumber);
        BlockLease lease = new BlockLease(sharedBlock, leaseNumber - 1);
        sharedBlock.setRowCount(leaseNumber);
        return lease;
    }

    /**
     * This leasing strategy optimizes for the create, return usecase it does not attempt to handle fragmentation
     * in any meaningful way beyond what the columnar nature of Arrow provides.
     *
     * @note In practice we see Markers get allocated and freed one at a time as they are used for Constraint Evaluation
     * so even this crude logic works well at present. As we improve the constraint system we expect to refactor the concept
     * of a Marker significantly to improve on this awkward lifecycle.
     */
    private synchronized void returnBlockLease(ArrowType type, int pos)
    {
        Block sharedBlock = sharedMarkerBlocks.get(type);
        Integer leaseNumber = markerLeases.get(type);

        if (sharedBlock != null && leaseNumber > 0 && leaseNumber == pos + 1) {
            markerLeases.put(type, leaseNumber - 1);
        }
    }

    @Override
    public void close()
            throws Exception
    {
        for (Block next : sharedMarkerBlocks.values()) {
            next.close();
        }

        sharedMarkerBlocks.clear();
        markerLeases.clear();
    }

    private static class BlockLease
    {
        private final Block block;
        private final int pos;

        public BlockLease(Block block, int pos)
        {
            this.block = block;
            this.pos = pos;
        }

        public Block getBlock()
        {
            return block;
        }

        public int getPos()
        {
            return pos;
        }
    }

    /**
     * Extends Marker with functionality to allow for sharing the same underlying Apache Arrow Block.
     */
    public class SharedBlockMarker
            extends Marker
    {
        private final MarkerFactory factory;
        private final int valuePosition;

        public SharedBlockMarker(MarkerFactory factory, Block block, int valuePosition, Bound bound, boolean nullValue)
        {
            super(block, valuePosition, bound, nullValue);
            this.factory = factory;
            this.valuePosition = valuePosition;
        }

        /**
         * Signals to the MarkerFactory that created this SharedBlockMarker that the row are valuePosition can be considered
         * free.
         */
        @Override
        public void close()
                throws Exception
        {
            //Don't call close on the super since we don't own the block, it shared.
            factory.returnBlockLease(getType(), valuePosition);
        }
    }
}

package com.amazonaws.athena.connector.lambda.domain.predicate;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.data.BlockUtils.setValue;

public class MarkerFactory
        implements AutoCloseable
{
    private final BlockAllocator allocator;
    private final Map<ArrowType, Block> sharedMarkerBlocks = new HashMap<>();
    private final Map<ArrowType, Integer> markerLeases = new HashMap<>();

    public MarkerFactory(BlockAllocator allocator)
    {
        this.allocator = allocator;
    }

    public Marker create(ArrowType type, Object value, Marker.Bound bound)
    {
        BlockLease lease = getOrCreateBlock(type);
        setValue(lease.getBlock().getFieldVector(Marker.DEFAULT_COLUMN), lease.getPos(), value);
        return new SharedBlockMarker(this, lease.getBlock(), lease.getPos(), bound, false);
    }

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

        @Override
        public void close()
                throws Exception
        {
            //Don't call close on the super since we don't own the block, it shared.
            factory.returnBlockLease(getType(), valuePosition);
        }
    }
}

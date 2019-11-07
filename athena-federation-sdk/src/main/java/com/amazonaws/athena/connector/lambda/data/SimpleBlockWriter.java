package com.amazonaws.athena.connector.lambda.data;

import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;

/**
 * Used to write a single Block using the BlockWriter programming model.
 *
 * @see BlockWriter
 */
public class SimpleBlockWriter
        implements BlockWriter
{
    private final Block block;

    /**
     * Basic constructor using a pre-allocated Block.
     *
     * @param block The Block to write into.
     */
    public SimpleBlockWriter(Block block)
    {
        this.block = block;
    }

    /**
     * Used to write rows into the Block that is managed by this BlockWriter.
     *
     * @param rowWriter The RowWriter that the BlockWriter should use to write rows into the Block(s) it is managing.
     * @See BlockWriter
     */
    public void writeRows(BlockWriter.RowWriter rowWriter)
    {
        int rowCount = block.getRowCount();

        int rows = rowWriter.writeRows(block, rowCount);

        if (rows > 0) {
            block.setRowCount(rowCount + rows);
        }
    }

    /**
     * Provides access to the ConstraintEvaluator that will be applied to the generated Blocks.
     *
     * @See BlockWriter
     */
    @Override
    public ConstraintEvaluator getConstraintEvaluator()
    {
        return block.getConstraintEvaluator();
    }
}

package com.amazonaws.athena.connector.lambda.data;

/**
 * Defines an abstraction that can be used to write to a Block without owning the lifecycle of the
 * Block.
 */
public interface BlockWriter
{
    /**
     * The interface you should implement for writing to a Block via
     * the inverted ownership model offered by BlockWriter.
     */
    interface RowWriter
    {
        /**
         * Used to accumulate rows as part of a block.
         *
         * @param block The block you can add your row to.
         * @param rowNum The row number in that block that the next row represents.
         * @return The number of rows that were added
         * @note We do not recommend writing more than 1 row per call. There are some use-cases which
         * are made much simpler by being able to write a small number (<100) rows per call. These often
         * relate to batched operators, scan side joins, or field expansions. Writing too many rows
         * will result in errors related to Block size management and are implementation specific.
         */
        int writeRows(Block block, int rowNum);
    }

    /**
     * Used to write rows via the BlockWriter.
     *
     * @param rowWriter The RowWriter that the BlockWriter should use to write rows into the Block(s) it is managing.
     */
    void writeRows(RowWriter rowWriter);
}

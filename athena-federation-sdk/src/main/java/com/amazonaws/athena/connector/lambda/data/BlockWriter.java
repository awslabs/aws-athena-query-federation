package com.amazonaws.athena.connector.lambda.data;

public interface BlockWriter
{
    interface RowWriter
    {
        /**
         * Used to accumulate rows as part of a block.
         *
         * @param block The block you can add your row to.
         * @param rowNum The row number in that block that the next row represents.
         * @return The number of rows that were added
         */
        int writeRows(Block block, int rowNum);
    }

    void writeRows(RowWriter rowWriter);
}

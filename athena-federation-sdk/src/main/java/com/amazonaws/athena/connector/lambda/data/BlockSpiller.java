package com.amazonaws.athena.connector.lambda.data;

import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;

import java.util.List;

public interface BlockSpiller
{
    interface RowWriter
    {
        /**
         * Used to accumulate rows as part of a response, with blocks being automatically spilled
         * as needed.
         *
         * @param block The block you can add your row to.
         * @param rowNum The row number in that block that the next row represents.
         * @return The number of rows that were added
         */
        int writeRows(Block block, int rowNum);
    }

    void writeRows(RowWriter rowWriter);

    boolean spilled();

    Block getBlock();

    List<SpillLocation> getSpillLocations();

    void close();
}

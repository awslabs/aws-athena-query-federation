package com.amazonaws.athena.connector.lambda.data;

public class SimpleBlockWriter
        implements BlockWriter
{
    private final Block block;

    public SimpleBlockWriter(Block block)
    {
        this.block = block;
    }

    public void writeRows(BlockWriter.RowWriter rowWriter)
    {
        int rowCount = block.getRowCount();

        int rows = rowWriter.writeRows(block, rowCount);

        if (rows > 0) {
            block.setRowCount(rowCount + rows);
        }
    }
}

package com.amazonaws.athena.connectors.gcs.common;

public class ColumnPrefix
{
    // Partition column name
    private String column;
    // Regular expression prefix for the column
    // So if the regular expression for a folder is (year=)(\d+) where (year=) is a capture group to capture
    // of a column prefix, this property will hold 'year='
    private String prefix;

    public ColumnPrefix(String column, String prefix)
    {
        this.column = column;
        this.prefix = prefix;
    }

    public String getColumn()
    {
        return column;
    }

    public ColumnPrefix column(String column)
    {
        this.column = column;
        return this;
    }

    public String getPrefix()
    {
        return prefix;
    }

    public ColumnPrefix prefix(String prefix)
    {
        this.prefix = prefix;
        return this;
    }
}

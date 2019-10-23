package com.amazonaws.athena.connector.lambda.records;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

import java.beans.Transient;

import static java.util.Objects.requireNonNull;

public class ReadRecordsResponse
        extends RecordResponse
{
    private final Block records;

    @JsonCreator
    public ReadRecordsResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("records") Block records)
    {
        super(RecordRequestType.READ_RECORDS, catalogName);
        requireNonNull(records, "records is null");
        this.records = records;
    }

    @Transient
    public Schema getSchema()
    {
        return records.getSchema();
    }

    @JsonProperty
    public Block getRecords()
    {
        return records;
    }

    @Transient
    public int getRecordCount()
    {
        return records.getRowCount();
    }

    @Override
    public void close()
            throws Exception
    {
        records.close();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("records", records)
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        ReadRecordsResponse that = (ReadRecordsResponse) o;

        return Objects.equal(this.records, that.records) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(records, getRequestType(), getCatalogName());
    }
}

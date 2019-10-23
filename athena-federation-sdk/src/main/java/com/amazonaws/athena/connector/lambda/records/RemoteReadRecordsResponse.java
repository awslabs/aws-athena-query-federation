package com.amazonaws.athena.connector.lambda.records;

import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

import java.beans.Transient;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class RemoteReadRecordsResponse
        extends RecordResponse
{
    private final Schema schema;
    private final List<SpillLocation> remoteBlocks;
    private final EncryptionKey encryptionKey;

    @JsonCreator
    public RemoteReadRecordsResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("schema") Schema schema,
            @JsonProperty("remoteBlocks") List<SpillLocation> remoteBlocks,
            @JsonProperty("encryptionKey") EncryptionKey encryptionKey)
    {
        super(RecordRequestType.READ_RECORDS, catalogName);
        requireNonNull(schema, "schema is null");
        requireNonNull(remoteBlocks, "remoteBlocks is null");
        this.schema = schema;
        this.remoteBlocks = Collections.unmodifiableList(remoteBlocks);
        this.encryptionKey = encryptionKey;
    }

    @JsonProperty
    public Schema getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<SpillLocation> getRemoteBlocks()
    {
        return remoteBlocks;
    }

    @Transient
    public int getNumberBlocks()
    {
        return remoteBlocks.size();
    }

    @JsonProperty
    public EncryptionKey getEncryptionKey()
    {
        return encryptionKey;
    }

    @Override
    public void close()
            throws Exception
    {
        //NoOp
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("schema", schema)
                .add("remoteBlocks", remoteBlocks)
                .add("encryptionKey", "XXXXXX")
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        RemoteReadRecordsResponse that = (RemoteReadRecordsResponse) o;

        return Objects.equal(this.schema, that.schema) &&
                Objects.equal(this.remoteBlocks, that.remoteBlocks) &&
                Objects.equal(this.encryptionKey, that.encryptionKey) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schema, remoteBlocks, encryptionKey, getRequestType(), getCatalogName());
    }
}

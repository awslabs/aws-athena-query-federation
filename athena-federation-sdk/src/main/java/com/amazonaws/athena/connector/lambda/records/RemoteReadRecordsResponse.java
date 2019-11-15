package com.amazonaws.athena.connector.lambda.records;

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

/**
 * Represents the output of a <code>ReadRecords</code> operation when the output has
 * spilled.
 */
public class RemoteReadRecordsResponse
        extends RecordResponse
{
    private final Schema schema;
    private final List<SpillLocation> remoteBlocks;
    private final EncryptionKey encryptionKey;

    /**
     * Constructs a new RemoteReadRecordsResponse object.
     *
     * @param catalogName The catalog name the data belongs to.
     * @param schema The schema of the spilled data.
     * @param remoteBlocks The locations of the spilled data.
     * @param encryptionKey The encryption key of the spilled data.
     */
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

    /**
     * Returns the schema of the spilled data.
     * @return The schema of the spilled data.
     */
    @JsonProperty
    public Schema getSchema()
    {
        return schema;
    }

    /**
     * Returns the locations of the spilled data.
     * @return The locations of the spilled data.
     */
    @JsonProperty
    public List<SpillLocation> getRemoteBlocks()
    {
        return remoteBlocks;
    }

    /**
     * Convenience accessor that returns the number of spilled blocks.
     *
     * @return The number of spilled blocks.
     */
    @Transient
    public int getNumberBlocks()
    {
        return remoteBlocks.size();
    }

    /**
     * Returns the encryption key of the spilled data.
     *
     * @return The encryption key of the spilled data.
     */
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

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

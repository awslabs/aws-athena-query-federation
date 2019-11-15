package com.amazonaws.athena.connector.lambda.data;

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

import static java.util.Objects.requireNonNull;

/**
 * Used to configure Spill functionality.
 */
public class SpillConfig
{
    //The default number of threads to use for async spill operations. 0 indicates that the calling thread should be used.
    private static final int DEFAULT_SPILL_THREADS = 1;
    //The encryption key that should be used to read/write spilled data. If null, encryption is disabled.
    private final EncryptionKey encryptionKey;
    //The location where the data is spilled.
    private final SpillLocation spillLocation;
    //The id of the request.
    private final String requestId;
    //The max bytes that can be in a single Block.
    private final long maxBlockBytes;
    //The max bytes that can be in an inline (non-spilled) Block.
    private final long maxInlineBlockSize;
    //The default number of threads to use for async spill operations. 0 indicates that the calling thread should be used.
    private final int numSpillThreads;

    private SpillConfig(Builder builder)
    {
        encryptionKey = builder.encryptionKey;
        spillLocation = requireNonNull(builder.spillLocation, "spillLocation was null");
        requestId = requireNonNull(builder.requestId, "requestId was null");
        maxBlockBytes = builder.maxBlockBytes;
        maxInlineBlockSize = builder.maxInlineBlockSize;
        numSpillThreads = builder.numSpillThreads;
    }

    /**
     * Gets the Encryption key to use when reading/writing data to the spill location.
     *
     * @return The EncryptionKey.
     * @note If null, spill encryption is disabled.
     */
    public EncryptionKey getEncryptionKey()
    {
        return encryptionKey;
    }

    /**
     * Gets the SpillLocation, if spill is enabled.
     *
     * @return The SpillLocation
     */
    public SpillLocation getSpillLocation()
    {
        return spillLocation;
    }

    /**
     * Gets the request Id, typically the Athena query ID.
     * @return The request id.
     */
    public String getRequestId()
    {
        return requestId;
    }

    /**
     * Gets max number of bytes a spilled Block can contain.
     * @return The number of bytes.
     */
    public long getMaxBlockBytes()
    {
        return maxBlockBytes;
    }

    /**
     * Gets max number of bytes an inline Block can contain.
     * @return The number of bytes.
     */
    public long getMaxInlineBlockSize()
    {
        return maxInlineBlockSize;
    }

    /**
     * Gets the number of threads the BlockSpiller can use.
     * @return The number of threads.
     */
    public int getNumSpillThreads()
    {
        return numSpillThreads;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static Builder newBuilder(SpillConfig copy)
    {
        Builder builder = new Builder();
        builder.encryptionKey = copy.getEncryptionKey();
        builder.maxBlockBytes = copy.getMaxBlockBytes();
        return builder;
    }

    public static final class Builder
    {
        private EncryptionKey encryptionKey;
        private String requestId;
        private SpillLocation spillLocation;
        private long maxBlockBytes;
        private long maxInlineBlockSize;
        private int numSpillThreads = DEFAULT_SPILL_THREADS;

        private Builder() {}

        public Builder withEncryptionKey(EncryptionKey val)
        {
            encryptionKey = val;
            return this;
        }

        public Builder withRequestId(String val)
        {
            requestId = val;
            return this;
        }

        public Builder withSpillLocation(SpillLocation val)
        {
            spillLocation = val;
            return this;
        }

        public Builder withNumSpillThreads(int val)
        {
            numSpillThreads = val;
            return this;
        }

        public Builder withMaxBlockBytes(long val)
        {
            maxBlockBytes = val;
            return this;
        }

        public Builder withMaxInlineBlockBytes(long val)
        {
            maxInlineBlockSize = val;
            return this;
        }

        public SpillConfig build()
        {
            return new SpillConfig(this);
        }
    }
}

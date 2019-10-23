package com.amazonaws.athena.connector.lambda.data;

import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;

import static java.util.Objects.requireNonNull;

public class SpillConfig
{
    private static final int DEFAULT_SPILL_THREADS = 1;

    private final EncryptionKey encryptionKey;
    private final SpillLocation spillLocation;
    private final String requestId;
    private final long maxBlockBytes;
    private final long maxInlineBlockSize;
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

    public EncryptionKey getEncryptionKey()
    {
        return encryptionKey;
    }

    public SpillLocation getSpillLocation()
    {
        return spillLocation;
    }

    public String getRequestId()
    {
        return requestId;
    }

    public long getMaxBlockBytes()
    {
        return maxBlockBytes;
    }

    public long getMaxInlineBlockSize()
    {
        return maxInlineBlockSize;
    }

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

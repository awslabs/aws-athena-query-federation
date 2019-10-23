package com.amazonaws.athena.connector.lambda.domain;

import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.Transient;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Split
{
    private final SpillLocation spillLocation;
    private final EncryptionKey encryptionKey;
    private final Map<String, String> properties;

    @JsonCreator
    public Split(@JsonProperty("spillLocation") SpillLocation spillLocation,
            @JsonProperty("encryptionKey") EncryptionKey encryptionKey,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.spillLocation = spillLocation;
        this.encryptionKey = encryptionKey;
        this.properties = Collections.unmodifiableMap(properties);
    }

    private Split(Builder builder)
    {
        this.properties = Collections.unmodifiableMap(builder.properties);
        this.spillLocation = builder.spillLocation;
        this.encryptionKey = builder.encryptionKey;
    }

    @Transient
    public String getProperty(String key)
    {
        return properties.get(key);
    }

    @Transient
    public int getPropertyAsInt(String key)
    {
        return Integer.parseInt(properties.get(key));
    }

    @Transient
    public long getPropertyAsLong(String key)
    {
        return Long.parseLong(properties.get(key));
    }

    @Transient
    public double getPropertyAsDouble(String key)
    {
        return Double.parseDouble(properties.get(key));
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @JsonProperty
    public SpillLocation getSpillLocation()
    {
        return spillLocation;
    }

    @JsonProperty
    public EncryptionKey getEncryptionKey()
    {
        return encryptionKey;
    }

    @Transient
    public static Builder newBuilder(SpillLocation spillLocation, EncryptionKey encryptionKey)
    {
        return new Builder().withSpillLocation(spillLocation).withEncryptionKey(encryptionKey);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        Split split = (Split) o;
        return Objects.equals(spillLocation, split.spillLocation) &&
                Objects.equals(encryptionKey, split.encryptionKey) &&
                Objects.equals(getProperties(), split.getProperties());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(spillLocation, encryptionKey, getProperties());
    }

    public static class Builder
    {
        private final Map<String, String> properties = new HashMap<>();
        private SpillLocation spillLocation;
        private EncryptionKey encryptionKey;

        private Builder() {}

        public Builder add(String key, String value)
        {
            properties.put(key, value);
            return this;
        }

        public Builder withSpillLocation(SpillLocation dir)
        {
            this.spillLocation = dir;
            return this;
        }

        public Builder withEncryptionKey(EncryptionKey key)
        {
            this.encryptionKey = key;
            return this;
        }

        public Split build()
        {
            return new Split(this);
        }
    }
}

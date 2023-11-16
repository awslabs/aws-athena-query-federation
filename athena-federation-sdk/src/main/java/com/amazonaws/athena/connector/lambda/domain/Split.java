package com.amazonaws.athena.connector.lambda.domain;

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

import java.beans.Transient;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A Split is best thought of as a unit of work that is part of a larger activity. For example, if we needed to
 * read a 100GB table stored in S3. We might want to improve performance by parallelizing the reads of this large file
 * by _splitting_ it up into 100MB pieces. You could think of each piece as a split. In general, Splits are opaque
 * to Athena with the exception of the SpillLocation and EncryptionKey which are used by Athena to find any data that
 * was spilled by the processing of the split. All properties on the split are soley produced by and consumed by the
 * connector.
 */
public class Split
{
    //The optional SpillLocation this Split can write to.
    private final SpillLocation spillLocation;
    //The optional EncryptionKey this Split can use to encrypt/decrypt data.
    private final EncryptionKey encryptionKey;
    //The properties that define what this split is meant to do.
    private final Map<String, String> properties;

    /**
     * Basic constructor.
     *
     * @param spillLocation The optional SpillLocation this Split can write to.
     * @param encryptionKey The optional EncryptionKey this Split can use to encrypt/decrypt data.
     * @param properties The properties that define what this split is meant to do.
     */
    @JsonCreator
    public Split(@JsonProperty("spillLocation") SpillLocation spillLocation,
            @JsonProperty("encryptionKey") EncryptionKey encryptionKey,
            @JsonProperty("properties") Map<String, String> properties)
    {
        requireNonNull(properties, "properties is null");
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

    /**
     * Retrieves the value of the requested property.
     *
     * @param key The name of the property to retrieve.
     * @return The value for that property or null if there is no such property.
     */
    @Transient
    public String getProperty(String key)
    {
        return properties.get(key);
    }

    /**
     * Retrieves the value of the requested property and attempts to parse the value into an int.
     *
     * @param key The name of the property to retrieve.
     * @return The value for that property, throws if there is no such property.
     */
    @Transient
    public int getPropertyAsInt(String key)
    {
        return Integer.parseInt(properties.get(key));
    }

    /**
     * Retrieves the value of the requested property and attempts to parse the value into an Long.
     *
     * @param key The name of the property to retrieve.
     * @return The value for that property, throws if there is no such property.
     */
    @Transient
    public long getPropertyAsLong(String key)
    {
        return Long.parseLong(properties.get(key));
    }

    /**
     * Retrieves the value of the requested property and attempts to parse the value into a double.
     *
     * @param key The name of the property to retrieve.
     * @return The value for that property, throws if there is no such property.
     */
    @Transient
    public double getPropertyAsDouble(String key)
    {
        return Double.parseDouble(properties.get(key));
    }

    /**
     * Provides access to all properties on this Split.
     *
     * @return Map<String, String> containing all properties on the split.
     */
    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    /**
     * The optional SpillLocation this Split can write to.
     *
     * @return The SpillLocation.
     */
    @JsonProperty
    public SpillLocation getSpillLocation()
    {
        return spillLocation;
    }

    /**
     * The optional EncryptionKey this Split can use to encrypt/decrypt data.
     *
     * @return The EncryptionKey.
     */
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
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

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("spillLocation", spillLocation)
                .add("encryptionKey", "<hidden>")
                .add("properties", properties)
                .toString();
    }

    public static class Builder
    {
        private final Map<String, String> properties = new HashMap<>();
        private SpillLocation spillLocation;
        private EncryptionKey encryptionKey;

        private Builder() {}

        /**
         * Adds the provided property key,value pair to the Split, overwriting any previous value for the key.
         *
         * @param key The key for the property.
         * @param value The value for the property.
         * @return The Builder itself.
         */
        public Builder add(String key, String value)
        {
            properties.put(key, value);
            return this;
        }

        /**
         * Sets the optional SpillLocation this Split can write to.
         *
         * @param val The SpillLocation
         * @return The Builder itself.
         */
        public Builder withSpillLocation(SpillLocation val)
        {
            this.spillLocation = val;
            return this;
        }

        /**
         * Sets the optional EncryptionKey this Split can use to encrypt/decrypt data.
         *
         * @param key The EncryptionKey
         * @return The Builder itself.
         */
        public Builder withEncryptionKey(EncryptionKey key)
        {
            this.encryptionKey = key;
            return this;
        }

        /**
         * Builds the Split
         *
         * @return A newly constructed Split using the attributes collected by this builder.
         */
        public Split build()
        {
            return new Split(this);
        }
    }
}

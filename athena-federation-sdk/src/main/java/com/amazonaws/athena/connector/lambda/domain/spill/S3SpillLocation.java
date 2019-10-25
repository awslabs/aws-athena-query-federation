package com.amazonaws.athena.connector.lambda.domain.spill;

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class S3SpillLocation
        implements SpillLocation
{
    private static final String SEPARATOR = "/";
    private final String bucket;
    private final String key;
    private final boolean directory;

    @JsonCreator
    public S3SpillLocation(@JsonProperty("bucket") String bucket,
            @JsonProperty("key") String key,
            @JsonProperty("directory") boolean directory)
    {
        this.bucket = bucket;
        this.key = key;
        this.directory = directory;
    }

    @JsonProperty
    public String getBucket()
    {
        return bucket;
    }

    @JsonProperty
    public String getKey()
    {
        return key;
    }

    @JsonProperty
    public boolean isDirectory()
    {
        return directory;
    }

    @Override
    public String toString()
    {
        return "S3SpillLocation{" +
                "bucket='" + bucket + '\'' +
                ", key='" + key + '\'' +
                ", directory=" + directory +
                '}';
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        S3SpillLocation that = (S3SpillLocation) o;
        return isDirectory() == that.isDirectory() &&
                Objects.equals(getBucket(), that.getBucket()) &&
                Objects.equals(getKey(), that.getKey());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getBucket(), getKey(), isDirectory());
    }

    public static class Builder
    {
        private String bucket;
        private String prefix;
        private String queryId;
        private String splitId;
        private boolean isDirectory = true;

        private Builder() {}

        public Builder withBucket(String bucket)
        {
            this.bucket = bucket;
            return this;
        }

        public Builder withPrefix(String prefix)
        {
            this.prefix = prefix;
            return this;
        }

        public Builder withIsDirectory(boolean isDirectory)
        {
            this.isDirectory = isDirectory;
            return this;
        }

        public Builder withQueryId(String queryId)
        {
            this.queryId = queryId;
            return this;
        }

        public Builder withSplitId(String splitId)
        {
            this.splitId = splitId;
            return this;
        }

        public S3SpillLocation build()
        {
            String key = prefix + SEPARATOR + queryId + SEPARATOR + splitId;
            return new S3SpillLocation(bucket, key, true);
        }
    }
}

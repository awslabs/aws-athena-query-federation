package com.amazonaws.athena.connector.lambda.metadata;

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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GetSplitsResponse
        extends MetadataResponse
{
    private final Set<Split> splits;
    private final String continuationToken;

    @JsonCreator
    public GetSplitsResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("splits") Set<Split> splits,
            @JsonProperty("continuationToken") String continuationToken)
    {
        super(MetadataRequestType.GET_SPLITS, catalogName);
        requireNonNull(splits, "splits is null");
        this.splits = Collections.unmodifiableSet(splits);
        this.continuationToken = continuationToken;
    }

    public GetSplitsResponse(String catalogName,
            Set<Split> splits)
    {
        super(MetadataRequestType.GET_SPLITS, catalogName);
        requireNonNull(splits, "splits is null");
        this.splits = Collections.unmodifiableSet(splits);
        this.continuationToken = null;
    }

    public GetSplitsResponse(String catalogName,
            Split split)
    {
        super(MetadataRequestType.GET_SPLITS, catalogName);
        requireNonNull(split, "split is null");
        Set<Split> splits = new HashSet<>();
        splits.add(split);
        this.splits = Collections.unmodifiableSet(splits);
        this.continuationToken = null;
    }

    @JsonProperty
    public Set<Split> getSplits()
    {
        return splits;
    }

    @JsonProperty
    public String getContinuationToken()
    {
        return continuationToken;
    }

    @Override
    public void close()
            throws Exception
    {
        //NoOp
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

        GetSplitsResponse that = (GetSplitsResponse) o;

        return Objects.equal(this.splits, that.splits) &&
                Objects.equal(this.continuationToken, that.continuationToken) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(splits, continuationToken, getRequestType(), getCatalogName());
    }

    @Override
    public String toString()
    {
        return "GetSplitsResponse{" +
                "splitSize=" + splits.size() +
                ", continuationToken='" + continuationToken + '\'' +
                '}';
    }
}

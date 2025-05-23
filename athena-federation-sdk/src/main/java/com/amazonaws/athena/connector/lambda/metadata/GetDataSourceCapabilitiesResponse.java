/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class GetDataSourceCapabilitiesResponse
        extends MetadataResponse
{
    private final Map<String, List<OptimizationSubType>> capabilities;
    /**
     * Constructs a new MetadataResponse object.
     *
     * @param catalogName The catalog name that the metadata is for.
     * @param capabilities The map of capabilities supported by the data source.
     */
    public GetDataSourceCapabilitiesResponse(@JsonProperty("catalogName") String catalogName,
                                             @JsonProperty("capabilities") Map<String, List<OptimizationSubType>> capabilities)
    {
        super(MetadataRequestType.GET_DATASOURCE_CAPABILITIES, catalogName);
        requireNonNull(capabilities, "capabilities are null");
        this.capabilities = Collections.unmodifiableMap(capabilities);
    }

    public Map<String, List<OptimizationSubType>> getCapabilities()
    {
        return Collections.unmodifiableMap(capabilities);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("capabilities", capabilities)
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .toString();
    }

    @Override
    public void close()
            throws Exception
    {
        //No Op
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

        GetDataSourceCapabilitiesResponse that = (GetDataSourceCapabilitiesResponse) o;

        return Objects.equal(this.capabilities, that.capabilities) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(capabilities, getRequestType(), getCatalogName());
    }
}

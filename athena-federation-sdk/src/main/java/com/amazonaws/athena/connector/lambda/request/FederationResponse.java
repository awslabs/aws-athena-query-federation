package com.amazonaws.athena.connector.lambda.request;

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

import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionResponse;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base class for all user facing responses.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ListSchemasResponse.class, name = "ListSchemasResponse"),
        @JsonSubTypes.Type(value = ListTablesResponse.class, name = "ListTablesResponse"),
        @JsonSubTypes.Type(value = GetTableResponse.class, name = "GetTableResponse"),
        @JsonSubTypes.Type(value = GetTableLayoutResponse.class, name = "GetTableLayoutResponse"),
        @JsonSubTypes.Type(value = GetSplitsResponse.class, name = "GetSplitsResponse"),
        @JsonSubTypes.Type(value = ReadRecordsResponse.class, name = "ReadRecordsResponse"),
        @JsonSubTypes.Type(value = RemoteReadRecordsResponse.class, name = "RemoteReadRecordsResponse"),
        @JsonSubTypes.Type(value = UserDefinedFunctionResponse.class, name = "UserDefinedFunctionResponse"),
        @JsonSubTypes.Type(value = PingResponse.class, name = "PingResponse")
})
public abstract class FederationResponse implements AutoCloseable
{
}

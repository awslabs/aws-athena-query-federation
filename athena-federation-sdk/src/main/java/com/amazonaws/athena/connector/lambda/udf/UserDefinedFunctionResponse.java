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
package com.amazonaws.athena.connector.lambda.udf;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class UserDefinedFunctionResponse extends FederationResponse
{
    private final Block records;
    private final String methodName;

    @JsonCreator
    public UserDefinedFunctionResponse(@JsonProperty("records") Block records,
                                       @JsonProperty("methodName") String methodName)
    {
        this.records = requireNonNull(records, "records is null");
        this.methodName = requireNonNull(methodName, "methodName is null");
    }

    @JsonProperty("records")
    public Block getRecords()
    {
        return records;
    }

    @JsonProperty("methodName")
    public String getMethodName()
    {
        return methodName;
    }

    @Override
    public void close() throws Exception
    {
        records.close();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UserDefinedFunctionResponse)) {
            return false;
        }
        UserDefinedFunctionResponse that = (UserDefinedFunctionResponse) o;
        return getRecords().equals(that.getRecords()) &&
                getMethodName().equals(that.getMethodName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getRecords(), getMethodName());
    }
}

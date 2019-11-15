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
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class UserDefinedFunctionRequest extends FederationRequest
{
    private final Block inputRecords;
    private final Schema outputSchema;
    private final String methodName;
    private final UserDefinedFunctionType functionType;

    @JsonCreator
    public UserDefinedFunctionRequest(@JsonProperty("identity") FederatedIdentity identity,
                                      @JsonProperty("inputRecords") Block inputRecords,
                                      @JsonProperty("outputSchema") Schema outputSchema,
                                      @JsonProperty("methodName") String methodName,
                                      @JsonProperty("functionType") UserDefinedFunctionType functionType)
    {
        super(identity);
        this.inputRecords = requireNonNull(inputRecords, "inputRecords is null");
        this.outputSchema = requireNonNull(outputSchema, "outputSchema is null");
        this.methodName = requireNonNull(methodName, "methodName is null");
        this.functionType = requireNonNull(functionType, "functionType is null");
    }

    @Override
    public void close() throws Exception
    {
        inputRecords.close();
    }

    @JsonProperty("inputRecords")
    public Block getInputRecords()
    {
        return inputRecords;
    }

    @JsonProperty("outputSchema")
    public Schema getOutputSchema()
    {
        return outputSchema;
    }

    @JsonProperty("methodName")
    public String getMethodName()
    {
        return methodName;
    }

    @JsonProperty("functionType")
    public UserDefinedFunctionType getFunctionType()
    {
        return functionType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UserDefinedFunctionRequest)) {
            return false;
        }
        UserDefinedFunctionRequest that = (UserDefinedFunctionRequest) o;
        return getInputRecords().equals(that.getInputRecords()) &&
                getOutputSchema().equals(that.getOutputSchema()) &&
                getMethodName().equals(that.getMethodName()) &&
                getFunctionType() == that.getFunctionType();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getInputRecords(), getOutputSchema(), getMethodName(), getFunctionType());
    }
}

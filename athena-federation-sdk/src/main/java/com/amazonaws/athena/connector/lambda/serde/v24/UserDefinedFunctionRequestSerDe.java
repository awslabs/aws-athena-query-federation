/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionType;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class UserDefinedFunctionRequestSerDe extends TypedSerDe<FederationRequest>
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String INPUT_RECORDS_FIELD = "inputRecords";
    private static final String OUTPUT_SCHEMA_FIELD = "outputSchema";
    private static final String METHOD_NAME_FIELD = "methodName";
    private static final String FUNCTION_TYPE_FIELD = "functionType";

    private final FederatedIdentitySerDe federatedIdentitySerDe;
    private final BlockSerDe blockSerDe;
    private final SchemaSerDe schemaSerDe;

    public UserDefinedFunctionRequestSerDe(FederatedIdentitySerDe federatedIdentitySerDe, BlockSerDe blockSerDe, SchemaSerDe schemaSerDe)
    {
        super(UserDefinedFunctionRequest.class);
        this.federatedIdentitySerDe = requireNonNull(federatedIdentitySerDe, "federatedIndentitySerDe is null");
        this.blockSerDe = requireNonNull(blockSerDe, "blockSerDe is null");
        this.schemaSerDe = requireNonNull(schemaSerDe, "schemaSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationRequest federationRequest)
            throws IOException
    {
        UserDefinedFunctionRequest userDefinedFunctionRequest = (UserDefinedFunctionRequest) federationRequest;

        jgen.writeFieldName(IDENTITY_FIELD);
        federatedIdentitySerDe.serialize(jgen, federationRequest.getIdentity());

        jgen.writeFieldName(INPUT_RECORDS_FIELD);
        blockSerDe.serialize(jgen, userDefinedFunctionRequest.getInputRecords());

        jgen.writeFieldName(OUTPUT_SCHEMA_FIELD);
        schemaSerDe.serialize(jgen, userDefinedFunctionRequest.getOutputSchema());

        jgen.writeStringField(METHOD_NAME_FIELD, userDefinedFunctionRequest.getMethodName());
        jgen.writeStringField(FUNCTION_TYPE_FIELD, userDefinedFunctionRequest.getFunctionType().toString());
    }

    @Override
    public UserDefinedFunctionRequest doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, IDENTITY_FIELD);
        FederatedIdentity identity = federatedIdentitySerDe.deserialize(jparser);

        assertFieldName(jparser, INPUT_RECORDS_FIELD);
        Block inputRecords = blockSerDe.deserialize(jparser);

        assertFieldName(jparser, OUTPUT_SCHEMA_FIELD);
        Schema outputSchema = schemaSerDe.deserialize(jparser);

        String methodName = getNextStringField(jparser, METHOD_NAME_FIELD);
        UserDefinedFunctionType functionType = UserDefinedFunctionType.valueOf(getNextStringField(jparser, FUNCTION_TYPE_FIELD));

        return new UserDefinedFunctionRequest(identity, inputRecords, outputSchema, methodName, functionType);
    }
}

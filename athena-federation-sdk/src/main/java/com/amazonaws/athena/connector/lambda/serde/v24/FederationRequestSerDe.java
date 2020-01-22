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

import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.DelegatingSerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.google.common.collect.ImmutableMap;

public class FederationRequestSerDe
        extends DelegatingSerDe<FederationRequest>
{
    public FederationRequestSerDe(
            PingRequestSerDe pingRequestSerDe,
            ListSchemasRequestSerDe listSchemasRequestSerDe,
            ListTablesRequestSerDe listTablesRequestSerDe,
            GetTableRequestSerDe getTableRequestSerDe,
            GetTableLayoutRequestSerDe getTableLayoutRequestSerDe,
            GetSplitsRequestSerDe getSplitsRequestSerDe,
            ReadRecordsRequestSerDe readRecordsRequestSerDe,
            UserDefinedFunctionRequestSerDe userDefinedFunctionRequestSerDe)
    {
        super(ImmutableMap.<String, TypedSerDe<FederationRequest>>builder()
                .put("PingRequest", pingRequestSerDe)
                .put("ListSchemasRequest", listSchemasRequestSerDe)
                .put("ListTablesRequest", listTablesRequestSerDe)
                .put("GetTableRequest", getTableRequestSerDe)
                .put("GetTableLayoutRequest", getTableLayoutRequestSerDe)
                .put("GetSplitsRequest", getSplitsRequestSerDe)
                .put("ReadRecordsRequest", readRecordsRequestSerDe)
                .put("UserDefinedFunctionRequest", userDefinedFunctionRequestSerDe)
                .build());
    }
}

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

import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.DelegatingSerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class FederationResponseSerDe extends DelegatingSerDe<FederationResponse>
{
    public FederationResponseSerDe(
            PingResponseSerDe pingResponseSerDe,
            ListSchemasResponseSerDe listSchemasResponseSerDe,
            ListTablesResponseSerDe listTablesResponseSerDe,
            GetTableResponseSerDe getTableResponseSerDe,
            GetTableLayoutResponseSerDe getTableLayoutResponseSerDe,
            GetSplitsResponseSerDe getSplitsResponseSerDe,
            ReadRecordsResponseSerDe readRecordsResponseSerDe,
            RemoteReadRecordsResponseSerDe remoteReadRecordsResponseSerDe,
            UserDefinedFunctionResponseSerDe userDefinedFunctionResponseSerDe)
    {
        super(ImmutableSet.<TypedSerDe<FederationResponse>>builder()
                .add(pingResponseSerDe)
                .add(listSchemasResponseSerDe)
                .add(listTablesResponseSerDe)
                .add(getTableResponseSerDe)
                .add(getTableLayoutResponseSerDe)
                .add(getSplitsResponseSerDe)
                .add(readRecordsResponseSerDe)
                .add(remoteReadRecordsResponseSerDe)
                .add(userDefinedFunctionResponseSerDe)
                .build());
    }
}

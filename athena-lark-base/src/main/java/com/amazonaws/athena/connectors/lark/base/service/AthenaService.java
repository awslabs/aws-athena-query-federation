/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.lark.base.service;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;

public class AthenaService
{
    private final AthenaClient athenaClient;

    public AthenaService()
    {
        this.athenaClient = getAthenaClient();
    }

    /**
     * Constructor with dependency injection for testing.
     *
     * @param athenaClient the AthenaClient to use
     */
    public AthenaService(AthenaClient athenaClient)
    {
        this.athenaClient = athenaClient;
    }

    protected AthenaClient getAthenaClient()
    {
        return AthenaClient.builder().build();
    }

    public String getAthenaQueryString(String queryId)
    {
        GetQueryExecutionResponse response = athenaClient.getQueryExecution(
                GetQueryExecutionRequest
                        .builder()
                        .queryExecutionId(queryId)
                        .build());
        return response.queryExecution().query();
    }
}

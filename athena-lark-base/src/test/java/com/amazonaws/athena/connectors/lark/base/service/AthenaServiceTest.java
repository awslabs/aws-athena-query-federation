
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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class AthenaServiceTest {

    @Test
    public void getAthenaQueryString_withInjectedClient_success() {
        // Test constructor with dependency injection
        AthenaClient athenaClient = Mockito.mock(AthenaClient.class);
        QueryExecution queryExecution = QueryExecution.builder().query("SELECT * FROM table").build();
        GetQueryExecutionResponse getQueryExecutionResponse = GetQueryExecutionResponse.builder()
                .queryExecution(queryExecution)
                .build();
        when(athenaClient.getQueryExecution(any(GetQueryExecutionRequest.class)))
                .thenReturn(getQueryExecutionResponse);

        AthenaService athenaService = new AthenaService(athenaClient);

        String result = athenaService.getAthenaQueryString("queryId");

        assertEquals("SELECT * FROM table", result);
    }

    @Test
    public void getAthenaQueryString_withDefaultConstructor_success() {
        // Test default constructor and getAthenaClient() method
        AthenaClient mockClient = Mockito.mock(AthenaClient.class);
        QueryExecution queryExecution = QueryExecution.builder().query("SELECT col FROM db.table").build();
        GetQueryExecutionResponse getQueryExecutionResponse = GetQueryExecutionResponse.builder()
                .queryExecution(queryExecution)
                .build();
        when(mockClient.getQueryExecution(any(GetQueryExecutionRequest.class)))
                .thenReturn(getQueryExecutionResponse);

        AthenaService athenaService = new AthenaService() {
            @Override
            protected AthenaClient getAthenaClient() {
                return mockClient;
            }
        };

        String result = athenaService.getAthenaQueryString("query-123");

        assertEquals("SELECT col FROM db.table", result);
    }

    @Test
    public void getAthenaClient_createsClientSuccessfully() throws Exception {
        // Use reflection to test the actual getAthenaClient() implementation
        // This covers the line: return AthenaClient.builder().build();
        AthenaService athenaService = new AthenaService() {
            // Don't override anything - we want to test the actual implementation
        };

        // Use reflection to call the protected method
        Method getAthenaClientMethod = AthenaService.class.getDeclaredMethod("getAthenaClient");
        getAthenaClientMethod.setAccessible(true);
        AthenaClient client = (AthenaClient) getAthenaClientMethod.invoke(athenaService);

        // Verify that a client was created
        assertNotNull(client, "AthenaClient should be created");

        // Clean up the client
        if (client != null) {
            client.close();
        }
    }
}

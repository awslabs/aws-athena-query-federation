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
package com.amazonaws.athena.connector.lambda;

import com.amazonaws.AmazonServiceException;
import com.google.common.collect.ImmutableList;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionStatus;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

import static com.amazonaws.athena.connector.lambda.handlers.AthenaExceptionFilter.ATHENA_EXCEPTION_FILTER;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryStatusCheckerTest
{
    private final ThrottlingInvoker athenaInvoker = ThrottlingInvoker.newDefaultBuilder(ATHENA_EXCEPTION_FILTER, com.google.common.collect.ImmutableMap.of()).build();

    @Mock
    private AthenaClient athena;

    @Test
    public void testFastTermination()
            throws InterruptedException
    {
        String queryId = "query0";
        GetQueryExecutionRequest request = GetQueryExecutionRequest.builder().queryExecutionId(queryId).build();
        when(athena.getQueryExecution(request)).thenReturn(GetQueryExecutionResponse.builder().queryExecution(QueryExecution.builder().status(QueryExecutionStatus.builder().state("FAILED").build()).build()).build());
        QueryStatusChecker queryStatusChecker = new QueryStatusChecker(athena, athenaInvoker, queryId);
        assertTrue(queryStatusChecker.isQueryRunning());
        Thread.sleep(2000);
        assertFalse(queryStatusChecker.isQueryRunning());
        verify(athena, times(1)).getQueryExecution(any(GetQueryExecutionRequest.class));
        queryStatusChecker.close();
    }

    @Test
    public void testSlowTermination()
            throws InterruptedException
    {
        String queryId = "query1";
        GetQueryExecutionRequest request = GetQueryExecutionRequest.builder().queryExecutionId(queryId).build();
        GetQueryExecutionResponse result1and2 = GetQueryExecutionResponse.builder().queryExecution(QueryExecution.builder().status(QueryExecutionStatus.builder().state("RUNNING").build()).build()).build();
        GetQueryExecutionResponse result3 = GetQueryExecutionResponse.builder().queryExecution(QueryExecution.builder().status(QueryExecutionStatus.builder().state("SUCCEEDED").build()).build()).build();
        when(athena.getQueryExecution(request)).thenReturn(result1and2).thenReturn(result1and2).thenReturn(result3);
        try (QueryStatusChecker queryStatusChecker = new QueryStatusChecker(athena, athenaInvoker, queryId)) {
            assertTrue(queryStatusChecker.isQueryRunning());
            Thread.sleep(2000);
            assertTrue(queryStatusChecker.isQueryRunning());
            Thread.sleep(3000);
            assertFalse(queryStatusChecker.isQueryRunning());
            verify(athena, times(3)).getQueryExecution(any(GetQueryExecutionRequest.class));
        }
    }

    @Test
    public void testNotFound()
            throws InterruptedException
    {
        String queryId = "query2";
        GetQueryExecutionRequest request = GetQueryExecutionRequest.builder().queryExecutionId(queryId).build();
        when(athena.getQueryExecution(request)).thenThrow(InvalidRequestException.builder().message("").build());
        try (QueryStatusChecker queryStatusChecker = new QueryStatusChecker(athena, athenaInvoker, queryId)) {
            assertTrue(queryStatusChecker.isQueryRunning());
            Thread.sleep(2000);
            assertTrue(queryStatusChecker.isQueryRunning());
            verify(athena, times(1)).getQueryExecution(any(GetQueryExecutionRequest.class));
        }
    }

    @Test
    public void testOtherError()
            throws InterruptedException
    {
        String queryId = "query3";
        GetQueryExecutionRequest request = GetQueryExecutionRequest.builder().queryExecutionId(queryId).build();
        when(athena.getQueryExecution(request)).thenThrow(new AmazonServiceException(""));
        try (QueryStatusChecker queryStatusChecker = new QueryStatusChecker(athena, athenaInvoker, queryId)) {
            assertTrue(queryStatusChecker.isQueryRunning());
            Thread.sleep(3000);
            assertTrue(queryStatusChecker.isQueryRunning());
            verify(athena, times(2)).getQueryExecution(any(GetQueryExecutionRequest.class));
        }
    }
}

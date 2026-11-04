/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connectors.cloudwatch.qpt.CloudwatchQueryPassthrough;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import org.mockito.ArgumentCaptor;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchUtilsTest {

    private static final String TEST_QUERY_ID = "test-query-id";
    private static final String TEST_QUERY_STRING = "fields @timestamp, @message";
    private static final String TEST_LOG_GROUP_1 = "/aws/lambda/test1";
    private static final String TEST_LOG_GROUP_2 = "/aws/lambda/test2";
    private static final String TEST_LOG_GROUPS = "\"" + TEST_LOG_GROUP_1 + "\", \"" + TEST_LOG_GROUP_2 + "\"";
    private static final long TEST_START_TIME = 1609459200000L;
    private static final long TEST_END_TIME = 1609545600000L;
    private static final int TEST_LIMIT = 100;

    @Mock
    private CloudWatchLogsClient mockAwsLogs;

    @Mock
    private ThrottlingInvoker mockInvoker;

    private Map<String, String> qptArguments;

    @Before
    public void setUp() {
        qptArguments = new HashMap<>();
        qptArguments.put(CloudwatchQueryPassthrough.STARTTIME, String.valueOf(TEST_START_TIME));
        qptArguments.put(CloudwatchQueryPassthrough.ENDTIME, String.valueOf(TEST_END_TIME));
        qptArguments.put(CloudwatchQueryPassthrough.QUERYSTRING, TEST_QUERY_STRING);
        qptArguments.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, TEST_LOG_GROUPS);
    }

    @Test
    public void startQueryRequest_withValidArguments_setsTimeRangeQueryStringAndLogGroups() {
        StartQueryRequest request = CloudwatchUtils.startQueryRequest(qptArguments);

        assertNotNull(request);
        assertEquals(TEST_START_TIME, (long) request.startTime());
        assertEquals(TEST_END_TIME, (long) request.endTime());
        assertEquals(TEST_QUERY_STRING, request.queryString());
        String[] expectedLogGroups = new String[]{TEST_LOG_GROUP_1, TEST_LOG_GROUP_2};
        assertArrayEquals(expectedLogGroups, request.logGroupNames().toArray(new String[0]));
    }

    @Test
    public void getQueryResult_withValidRequest_returnsResponse() {
        StartQueryRequest request = CloudwatchUtils.startQueryRequest(qptArguments);
        StartQueryResponse mockResponse = StartQueryResponse.builder().queryId(TEST_QUERY_ID).build();

        when(mockAwsLogs.startQuery(any(StartQueryRequest.class))).thenReturn(mockResponse);

        StartQueryResponse response = CloudwatchUtils.getQueryResult(mockAwsLogs, request);

        // Verify that the method returns the exact response from the CloudWatch client
        assertEquals(mockResponse, response);

        // Verify that the CloudWatch client was called with the exact request object
        verify(mockAwsLogs).startQuery(request);
    }

    @Test
    public void getQueryResults_withValidRequest_constructsRequestWithQueryIdAndReturnsResponse() {
        StartQueryResponse startQueryResponse = StartQueryResponse.builder().queryId(TEST_QUERY_ID).build();
        GetQueryResultsResponse mockResults = GetQueryResultsResponse.builder()
                .status(QueryStatus.COMPLETE)
                .build();

        when(mockAwsLogs.getQueryResults(any(GetQueryResultsRequest.class))).thenReturn(mockResults);

        GetQueryResultsResponse response = CloudwatchUtils.getQueryResults(mockAwsLogs, startQueryResponse);

        // Verify that the method returns the exact response from the CloudWatch client
        assertEquals(mockResults, response);

        // Verify that the CloudWatch client was called with a request containing the correct query ID
        ArgumentCaptor<GetQueryResultsRequest> requestCaptor = ArgumentCaptor.forClass(GetQueryResultsRequest.class);
        verify(mockAwsLogs).getQueryResults(requestCaptor.capture());
        assertEquals(TEST_QUERY_ID, requestCaptor.getValue().queryId());
    }

    @Test
    public void getResult_withValidArguments_returnsCorrectResponse() throws TimeoutException, InterruptedException  {
            StartQueryResponse startQueryResponse = StartQueryResponse.builder().queryId(TEST_QUERY_ID).build();
            GetQueryResultsResponse runningResponse = GetQueryResultsResponse.builder()
                    .status(QueryStatus.RUNNING)
                    .build();
            GetQueryResultsResponse completeResponse = GetQueryResultsResponse.builder()
                    .status(QueryStatus.COMPLETE)
                    .build();

        // Execute the callable so that getResult's logic runs and the CW client is invoked with real requests
        when(mockInvoker.invoke(any())).thenAnswer(invocation -> invocation.getArgument(0, Callable.class).call());

        when(mockAwsLogs.startQuery(any(StartQueryRequest.class))).thenReturn(startQueryResponse);
        when(mockAwsLogs.getQueryResults(any(GetQueryResultsRequest.class))).thenReturn(runningResponse, completeResponse);

            GetQueryResultsResponse response = CloudwatchUtils.getResult(mockInvoker, mockAwsLogs, qptArguments, TEST_LIMIT);

        // Verify correct response is returned
        assertEquals(completeResponse, response);

        // Verify CW client was called with the correct StartQueryRequest (from qptArguments and limit)
        ArgumentCaptor<StartQueryRequest> startRequestCaptor = ArgumentCaptor.forClass(StartQueryRequest.class);
        verify(mockAwsLogs).startQuery(startRequestCaptor.capture());
        StartQueryRequest startRequest = startRequestCaptor.getValue();
        assertEquals(TEST_QUERY_STRING, startRequest.queryString());
        assertArrayEquals(new String[]{TEST_LOG_GROUP_1, TEST_LOG_GROUP_2}, startRequest.logGroupNames().toArray(new String[0]));
        assertEquals(TEST_LIMIT, (int) startRequest.limit());

        // Verify getQueryResults was called with the correct query ID and that we poll until COMPLETE (2 calls: RUNNING then COMPLETE)
        ArgumentCaptor<GetQueryResultsRequest> getResultsCaptor = ArgumentCaptor.forClass(GetQueryResultsRequest.class);
        verify(mockAwsLogs, times(2)).getQueryResults(getResultsCaptor.capture());
        assertEquals(TEST_QUERY_ID, getResultsCaptor.getAllValues().get(0).queryId());
        assertEquals(TEST_QUERY_ID, getResultsCaptor.getAllValues().get(1).queryId());
    }

    @Test
    public void getQueryResults_withNullResponse_returnsNull() {
        when(mockAwsLogs.getQueryResults(any(GetQueryResultsRequest.class))).thenReturn(null);

        StartQueryResponse startQueryResponse = StartQueryResponse.builder().queryId(TEST_QUERY_ID).build();
        GetQueryResultsResponse response = CloudwatchUtils.getQueryResults(mockAwsLogs, startQueryResponse);

        assertNull(response);
        verify(mockAwsLogs).getQueryResults(any(GetQueryResultsRequest.class));
    }

    @Test
    public void startQueryRequest_withEndTimeBeforeStartTime_passesTimesWithoutValidation() {
        Map<String, String> invalidTimeRangeArgs = new HashMap<>();
        // End time is before start time (invalid range)
        invalidTimeRangeArgs.put(CloudwatchQueryPassthrough.STARTTIME, String.valueOf(TEST_END_TIME));
        invalidTimeRangeArgs.put(CloudwatchQueryPassthrough.ENDTIME, String.valueOf(TEST_START_TIME));
        invalidTimeRangeArgs.put(CloudwatchQueryPassthrough.QUERYSTRING, TEST_QUERY_STRING);
        invalidTimeRangeArgs.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, TEST_LOG_GROUPS);

        // Should still create the request (validation happens at CloudWatch API level)
        StartQueryRequest request = CloudwatchUtils.startQueryRequest(invalidTimeRangeArgs);
        assertNotNull(request);
        assertEquals(TEST_END_TIME, (long) request.startTime());
        assertEquals(TEST_START_TIME, (long) request.endTime());
    }

    @Test
    public void startQueryRequest_withEmptyLogGroupNames_createsRequestWithSingleEmptyLogGroup() {
        Map<String, String> emptyLogGroupArgs = new HashMap<>();
        emptyLogGroupArgs.put(CloudwatchQueryPassthrough.STARTTIME, String.valueOf(TEST_START_TIME));
        emptyLogGroupArgs.put(CloudwatchQueryPassthrough.ENDTIME, String.valueOf(TEST_END_TIME));
        emptyLogGroupArgs.put(CloudwatchQueryPassthrough.QUERYSTRING, TEST_QUERY_STRING);
        emptyLogGroupArgs.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, "");

        // Should handle empty log group names gracefully
        StartQueryRequest request = CloudwatchUtils.startQueryRequest(emptyLogGroupArgs);
        assertNotNull(request);
        assertNotNull(request.logGroupNames());
        assertEquals(1, request.logGroupNames().size());
    }

    @Test
    public void startQueryRequest_withSingleLogGroup_removesLeadingAndTrailingQuotes() {
        Map<String, String> singleLogGroupArgs = new HashMap<>();
        singleLogGroupArgs.put(CloudwatchQueryPassthrough.STARTTIME, String.valueOf(TEST_START_TIME));
        singleLogGroupArgs.put(CloudwatchQueryPassthrough.ENDTIME, String.valueOf(TEST_END_TIME));
        singleLogGroupArgs.put(CloudwatchQueryPassthrough.QUERYSTRING, TEST_QUERY_STRING);
        singleLogGroupArgs.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, "\"/aws/lambda/single\"");

        StartQueryRequest request = CloudwatchUtils.startQueryRequest(singleLogGroupArgs);
        assertNotNull(request);
        assertEquals(1, request.logGroupNames().size());
        assertEquals("/aws/lambda/single", request.logGroupNames().get(0));
    }

    @Test
    public void startQueryRequest_withEmptyQueryString_createsRequestWithEmptyQueryString() {
        Map<String, String> emptyQueryArgs = new HashMap<>();
        emptyQueryArgs.put(CloudwatchQueryPassthrough.STARTTIME, String.valueOf(TEST_START_TIME));
        emptyQueryArgs.put(CloudwatchQueryPassthrough.ENDTIME, String.valueOf(TEST_END_TIME));
        emptyQueryArgs.put(CloudwatchQueryPassthrough.QUERYSTRING, "");
        emptyQueryArgs.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, TEST_LOG_GROUPS);

        StartQueryRequest request = CloudwatchUtils.startQueryRequest(emptyQueryArgs);
        assertNotNull(request);
        assertEquals("", request.queryString());
    }
}

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
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

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
    public void testStartQueryRequest() {
        StartQueryRequest request = CloudwatchUtils.startQueryRequest(qptArguments);

        assertNotNull(request);
        assertEquals(TEST_START_TIME, (long) request.startTime());
        assertEquals(TEST_END_TIME, (long) request.endTime());
        assertEquals(TEST_QUERY_STRING, request.queryString());
        String[] expectedLogGroups = new String[]{TEST_LOG_GROUP_1, TEST_LOG_GROUP_2};
        assertArrayEquals(expectedLogGroups, request.logGroupNames().toArray(new String[0]));
    }

    @Test
    public void testGetQueryResult() {
        StartQueryRequest request = CloudwatchUtils.startQueryRequest(qptArguments);
        StartQueryResponse mockResponse = StartQueryResponse.builder().queryId(TEST_QUERY_ID).build();

        when(mockAwsLogs.startQuery(any(StartQueryRequest.class))).thenReturn(mockResponse);

        StartQueryResponse response = CloudwatchUtils.getQueryResult(mockAwsLogs, request);

        assertNotNull(response);
        assertEquals(TEST_QUERY_ID, response.queryId());
        verify(mockAwsLogs).startQuery(request);
    }

    @Test
    public void testGetQueryResults() {
        StartQueryResponse startQueryResponse = StartQueryResponse.builder().queryId(TEST_QUERY_ID).build();
        GetQueryResultsResponse mockResults = GetQueryResultsResponse.builder()
                .status(QueryStatus.COMPLETE)
                .build();

        when(mockAwsLogs.getQueryResults(any(GetQueryResultsRequest.class))).thenReturn(mockResults);

        GetQueryResultsResponse response = CloudwatchUtils.getQueryResults(mockAwsLogs, startQueryResponse);

        assertNotNull(response);
        assertEquals(QueryStatus.COMPLETE, response.status());
        verify(mockAwsLogs).getQueryResults(any(GetQueryResultsRequest.class));
    }

    @Test
    public void testGetResult() throws TimeoutException, InterruptedException {
            StartQueryResponse startQueryResponse = StartQueryResponse.builder().queryId(TEST_QUERY_ID).build();
            GetQueryResultsResponse mockResults = GetQueryResultsResponse.builder()
                    .status(QueryStatus.COMPLETE)
                    .build();

            when(mockInvoker.invoke(any())).thenReturn(startQueryResponse).thenReturn(mockResults);

            GetQueryResultsResponse response = CloudwatchUtils.getResult(mockInvoker, mockAwsLogs, qptArguments, TEST_LIMIT);

            assertNotNull(response);
            assertEquals(QueryStatus.COMPLETE, response.status());
            verify(mockInvoker, times(2)).invoke(any());
    }

    @Test
    public void testGetQueryResultsWithNullResponse() {
        when(mockAwsLogs.getQueryResults(any(GetQueryResultsRequest.class))).thenReturn(null);

        StartQueryResponse startQueryResponse = StartQueryResponse.builder().queryId(TEST_QUERY_ID).build();
        GetQueryResultsResponse response = CloudwatchUtils.getQueryResults(mockAwsLogs, startQueryResponse);

        assertNull(response);
        verify(mockAwsLogs).getQueryResults(any(GetQueryResultsRequest.class));
    }
}

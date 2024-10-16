/*-
 * #%L
 * athena-cloudwatch
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryResponse;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public final class CloudwatchUtils
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchUtils.class);
    public static final int RESULT_TIMEOUT = 10;
    private CloudwatchUtils() {}
    public static StartQueryRequest startQueryRequest(Map<String, String> qptArguments)
    {
        return StartQueryRequest.builder().endTime(Long.valueOf(qptArguments.get(CloudwatchQueryPassthrough.ENDTIME))).startTime(Long.valueOf(qptArguments.get(CloudwatchQueryPassthrough.STARTTIME)))
                .queryString(qptArguments.get(CloudwatchQueryPassthrough.QUERYSTRING)).logGroupNames(getLogGroupNames(qptArguments)).build();
    }

    private static String[] getLogGroupNames(Map<String, String> qptArguments)
    {
        String[] logGroupNames = qptArguments.get(CloudwatchQueryPassthrough.LOGGROUPNAMES).split(", ");
        logger.info("log group names {}", logGroupNames);
        for (int i = 0; i < logGroupNames.length; i++) {
            logGroupNames[i] = logGroupNames[i].replaceAll("^\"|\"$", "");
        }
        return logGroupNames;
    }

    public static StartQueryResponse getQueryResult(CloudWatchLogsClient awsLogs, StartQueryRequest startQueryRequest)
    {
        return awsLogs.startQuery(startQueryRequest);
    }

    public static GetQueryResultsResponse getQueryResults(CloudWatchLogsClient awsLogs, StartQueryResponse startQueryResponse)
    {
        return awsLogs.getQueryResults(GetQueryResultsRequest.builder().queryId(startQueryResponse.queryId()).build());
    }

    public static GetQueryResultsResponse getResult(ThrottlingInvoker invoker, CloudWatchLogsClient awsLogs, Map<String, String> qptArguments, int limit) throws TimeoutException, InterruptedException
    {
        StartQueryResponse startQueryResponse = invoker.invoke(() -> getQueryResult(awsLogs, startQueryRequest(qptArguments).toBuilder().limit(limit).build()));
        QueryStatus status = null;
        GetQueryResultsResponse getQueryResultsResponse;
        Instant startTime = Instant.now(); // Record the start time
        do {
            getQueryResultsResponse = invoker.invoke(() -> getQueryResults(awsLogs, startQueryResponse));
            status = getQueryResultsResponse.status();
            Thread.sleep(1000);

            // Check if 10 minutes have passed
            Instant currentTime = Instant.now();
            long elapsedMinutes = ChronoUnit.MINUTES.between(startTime, currentTime);
            if (elapsedMinutes >= RESULT_TIMEOUT) {
                throw new RuntimeException("Query execution timeout exceeded.");
            }
        } while (!status.equals(QueryStatus.COMPLETE));

        return getQueryResultsResponse;
    }
}

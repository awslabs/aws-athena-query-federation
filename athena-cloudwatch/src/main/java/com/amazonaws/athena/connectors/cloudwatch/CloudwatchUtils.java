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
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.GetQueryResultsRequest;
import com.amazonaws.services.logs.model.GetQueryResultsResult;
import com.amazonaws.services.logs.model.StartQueryRequest;
import com.amazonaws.services.logs.model.StartQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return new StartQueryRequest().withEndTime(Long.valueOf(qptArguments.get(CloudwatchQueryPassthrough.ENDTIME))).withStartTime(Long.valueOf(qptArguments.get(CloudwatchQueryPassthrough.STARTTIME)))
                .withQueryString(qptArguments.get(CloudwatchQueryPassthrough.QUERYSTRING)).withLogGroupNames(getLogGroupNames(qptArguments));
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

    public static StartQueryResult getQueryResult(AWSLogs awsLogs, StartQueryRequest startQueryRequest)
    {
        return awsLogs.startQuery(startQueryRequest);
    }

    public static GetQueryResultsResult getQueryResults(AWSLogs awsLogs, StartQueryResult startQueryResult)
    {
        return awsLogs.getQueryResults(new GetQueryResultsRequest().withQueryId(startQueryResult.getQueryId()));
    }

    public static GetQueryResultsResult getResult(ThrottlingInvoker invoker, AWSLogs awsLogs, Map<String, String> qptArguments, int limit) throws TimeoutException, InterruptedException
    {
        StartQueryResult startQueryResult = invoker.invoke(() -> getQueryResult(awsLogs, startQueryRequest(qptArguments).withLimit(limit)));
        String status = null;
        GetQueryResultsResult getQueryResultsResult;
        Instant startTime = Instant.now(); // Record the start time
        do {
            getQueryResultsResult = invoker.invoke(() -> getQueryResults(awsLogs, startQueryResult));
            status = getQueryResultsResult.getStatus();
            Thread.sleep(1000);

            // Check if 10 minutes have passed
            Instant currentTime = Instant.now();
            long elapsedMinutes = ChronoUnit.MINUTES.between(startTime, currentTime);
            if (elapsedMinutes >= RESULT_TIMEOUT) {
                throw new RuntimeException("Query execution timeout exceeded.");
            }
        } while (!status.equalsIgnoreCase("Complete"));

        return getQueryResultsResult;
    }
}

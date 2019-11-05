/*-
 * #%L
 * athena-cloudwatch-metrics
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
package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.services.cloudwatch.model.AmazonCloudWatchException;
import com.amazonaws.services.cloudwatch.model.LimitExceededException;

/**
 * Used to identify Exceptions that are related to Cloudwatch Metrics throttling events.
 */
public class MetricsExceptionFilter
        implements ThrottlingInvoker.ExceptionFilter
{
    public static final ThrottlingInvoker.ExceptionFilter EXCEPTION_FILTER = new MetricsExceptionFilter();

    private MetricsExceptionFilter() {}

    @Override
    public boolean isMatch(Exception ex)
    {
        if (ex instanceof AmazonCloudWatchException && ex.getMessage().startsWith("Rate exceeded")) {
            return true;
        }

        if (ex instanceof AmazonCloudWatchException && ex.getMessage().startsWith("Request has been throttled")) {
            return true;
        }

        return (ex instanceof LimitExceededException);
    }
}

/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.services.logs.model.AWSLogsException;
import com.amazonaws.services.logs.model.LimitExceededException;

/**
 * Used to identify Exceptions that are related to Cloudwatch Logs throttling events.
 */
public class CloudwatchExceptionFilter
        implements ThrottlingInvoker.ExceptionFilter
{
    public static final ThrottlingInvoker.ExceptionFilter EXCEPTION_FILTER = new CloudwatchExceptionFilter();

    private CloudwatchExceptionFilter() {}

    @Override
    public boolean isMatch(Exception ex)
    {
        if (ex instanceof AWSLogsException && ex.getMessage().startsWith("Rate exceeded")) {
            return true;
        }

        return (ex instanceof LimitExceededException);
    }
}

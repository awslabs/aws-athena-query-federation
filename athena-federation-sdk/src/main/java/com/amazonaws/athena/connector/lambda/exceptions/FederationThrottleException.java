package com.amazonaws.athena.connector.lambda.exceptions;

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

/**
 * Throw this exception if your source is unable to keep up with the rate or concurrency
 * of requests. Athena constantly monitors for performance of federated sources and
 * employs a congestion control mechanism to reduce pressure on sources that may be
 * overwhelmed or unable to keep up. Throwing this exception gives Athena important
 * back pressure information. Alternatively you can reduce the concurrency of the
 * affected Lambda function in the Lambda console which will cause Lambda to generate
 * Throttle exceptions for Athena.
 *
 * @note If you throw this exception after writing any data Athena may fail the operation
 *       to ensure consistency of your results. This is because Athena eagerly processes
 *       your results but is unsure if two identical calls to your source will produce
 *       the exact same result set (including ordering).
 */
public class FederationThrottleException
        extends RuntimeException
{
    public FederationThrottleException()
    {
        super();
    }

    public FederationThrottleException(String message)
    {
        super(message);
    }

    public FederationThrottleException(String message, Throwable cause)
    {
        super(message, cause);
    }
}

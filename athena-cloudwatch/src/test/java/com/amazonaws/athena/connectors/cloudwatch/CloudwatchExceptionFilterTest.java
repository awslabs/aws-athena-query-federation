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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatch.model.LimitExceededException;
import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchExceptionFilter.EXCEPTION_FILTER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchExceptionFilterTest
{
    @Test
    public void isMatch_withCloudWatchLogsExceptionAndRateExceeded_returnsTrue()
    {
        CloudWatchLogsException exception = (CloudWatchLogsException) CloudWatchLogsException.builder()
                .message("Rate exceeded")
                .build();
        
        boolean match = EXCEPTION_FILTER.isMatch(exception);
        assertTrue("Should match CloudWatchLogsException with 'Rate exceeded' message", match);
    }

    @Test
    public void isMatch_withCloudWatchLogsExceptionAndDifferentMessage_returnsFalse()
    {
        CloudWatchLogsException exception = (CloudWatchLogsException) CloudWatchLogsException.builder()
                .message("Invalid parameter")
                .build();
        
        boolean match = EXCEPTION_FILTER.isMatch(exception);
        assertFalse("Should not match CloudWatchLogsException with different message", match);
    }

    @Test
    public void isMatch_withNullException_returnsFalse()
    {
        boolean match = EXCEPTION_FILTER.isMatch(null);
        assertFalse("Should not match null exception", match);
    }

    @Test
    public void isMatch_withLimitExceededException_returnsTrue()
    {
        LimitExceededException limitExceededException = LimitExceededException.builder()
                .message("Limit exceeded")
                .build();

        boolean match = EXCEPTION_FILTER.isMatch(limitExceededException);
        assertTrue("Should match LimitExceededException", match);
    }
}
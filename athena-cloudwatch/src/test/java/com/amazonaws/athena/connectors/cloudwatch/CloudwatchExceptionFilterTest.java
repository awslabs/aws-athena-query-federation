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
import org.mockito.Mock;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatch.model.LimitExceededException;
import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchExceptionFilter.EXCEPTION_FILTER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchExceptionFilterTest
{
    @Mock
    private CloudWatchLogsException mockCloudWatchLogsException;

    @Test
    public void testIsMatch_CloudWatchLogsExceptionWithRateExceeded()
    {
        when(mockCloudWatchLogsException.getMessage()).thenReturn("Rate exceeded");
        
        boolean match = EXCEPTION_FILTER.isMatch(mockCloudWatchLogsException);
        assertTrue("Should match CloudWatchLogsException with 'Rate exceeded' message", match);
    }

    @Test
    public void testIsMatch_CloudWatchLogsExceptionWithDifferentMessage()
    {
        when(mockCloudWatchLogsException.getMessage()).thenReturn("Invalid parameter");
        
        boolean match = EXCEPTION_FILTER.isMatch(mockCloudWatchLogsException);
        assertFalse("Should not match CloudWatchLogsException with different message", match);
    }

    @Test
    public void testIsMatch_NullException()
    {
        boolean match = EXCEPTION_FILTER.isMatch(null);
        assertFalse("Should not match null exception", match);
    }

    @Test
    public void testIsMatch_LimitExceededException()
    {
        LimitExceededException limitExceededException = LimitExceededException.builder()
                .message("Limit exceeded")
                .build();

        boolean match = EXCEPTION_FILTER.isMatch(limitExceededException);
        assertTrue("Should match LimitExceededException", match);
    }
}
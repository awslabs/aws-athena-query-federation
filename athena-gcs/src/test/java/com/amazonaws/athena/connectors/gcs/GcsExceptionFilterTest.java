/*-
 * #%L
 * Amazon Athena GCS Connector
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.model.AthenaException;

import static com.amazonaws.athena.connectors.gcs.GcsThrottlingExceptionFilter.EXCEPTION_FILTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class GcsExceptionFilterTest
{
    @Test
    public void testIsMatch()
    {
        boolean match = EXCEPTION_FILTER.isMatch(AthenaException.builder().message("Rate exceeded").build());
        assertTrue(match);
        boolean match1 = EXCEPTION_FILTER.isMatch(AthenaException.builder().message("Too Many Requests").build());
        assertTrue(match1);
        boolean match3 = EXCEPTION_FILTER.isMatch(AthenaException.builder().message("other").build());
        assertFalse(match3);
    }
}

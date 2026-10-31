/*-
 * #%L
 * athena-lark-base
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
package com.amazonaws.athena.connectors.lark.base.throttling;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.model.TooManyRequestsException;
import software.amazon.awssdk.services.glue.model.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseExceptionFilterTest {

    private final BaseExceptionFilter filter = new BaseExceptionFilter();

    @Test
    public void testIsMatch_NullMessage() {
        assertFalse(filter.isMatch(new Exception()));
    }

    @Test
    public void testIsMatch_ThrottlingMessages() {
        assertTrue(filter.isMatch(new Exception("1254290")));
        assertTrue(filter.isMatch(new Exception("TooManyRequest")));
        assertTrue(filter.isMatch(new Exception("1254291")));
        assertTrue(filter.isMatch(new Exception("Write conflict")));
        assertTrue(filter.isMatch(new Exception("1255040")));
        assertTrue(filter.isMatch(new Exception("Request timed out")));
        assertTrue(filter.isMatch(new Exception("1254607")));
        assertTrue(filter.isMatch(new Exception("Data not ready")));
        assertTrue(filter.isMatch(new Exception("1254036")));
        assertTrue(filter.isMatch(new Exception("Base is copying")));
    }

    @Test
    public void testIsMatch_NonThrottlingMessage() {
        assertFalse(filter.isMatch(new Exception("Some other error")));
    }

    @Test
    public void testIsMatch_ThrottlingExceptions() {
        assertTrue(filter.isMatch(TooManyRequestsException.builder().build()));
        assertTrue(filter.isMatch(ConcurrentModificationException.builder().build()));
        assertTrue(filter.isMatch(ConcurrentRunsExceededException.builder().build()));
        assertTrue(filter.isMatch(ConflictException.builder().build()));
        assertTrue(filter.isMatch(IntegrationConflictOperationException.builder().build()));
        assertTrue(filter.isMatch(IntegrationQuotaExceededException.builder().build()));
        assertTrue(filter.isMatch(OperationTimeoutException.builder().build()));
        assertTrue(filter.isMatch(ResourceNotReadyException.builder().build()));
        assertTrue(filter.isMatch(ResourceNumberLimitExceededException.builder().build()));
        assertTrue(filter.isMatch(ThrottlingException.builder().build()));
    }

    @Test
    public void testIsMatch_NonThrottlingException() {
        assertFalse(filter.isMatch(new RuntimeException("error")));
    }
}

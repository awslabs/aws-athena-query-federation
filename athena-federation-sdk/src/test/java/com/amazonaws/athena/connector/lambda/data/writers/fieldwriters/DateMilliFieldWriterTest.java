/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.data.writers.fieldwriters;

import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureDateMilliExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DateMilliFieldWriterTest {

    private static final String vectorName = "testVector";
    private static final long expectedEpochMilliseconds = 1672531200000L;
    private static final LocalDateTime expectedDate = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(expectedEpochMilliseconds), ZoneOffset.UTC);

    private BufferAllocator allocator;
    private DateMilliVector vector;
    private DateMilliExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private DateMilliFieldWriter dateMilliFieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new DateMilliVector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(DateMilliExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        dateMilliFieldWriter = new DateMilliFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedEpochMilliseconds, vector.get(0));
    }

    @Test
    public void write_withValidDateMilliValue_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(expectedDate)).thenReturn(true);
            configureDateMilliExtractor(mockExtractor, expectedEpochMilliseconds, 1);

            boolean result = dateMilliFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableDateMilliHolder.class));
            verify(mockConstraintProjector, times(1)).apply(expectedDate);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedDate)).thenReturn(false);
        configureDateMilliExtractor(mockExtractor, expectedEpochMilliseconds, 1);

        boolean result = dateMilliFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableDateMilliHolder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedDate);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {
            dateMilliFieldWriter = new DateMilliFieldWriter(mockExtractor, vector, null);
            configureDateMilliExtractor(mockExtractor, expectedEpochMilliseconds, 1);

            boolean result = dateMilliFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableDateMilliHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullDateMilliValue_shouldMarkVectorAsNull() throws Exception {
        configureDateMilliExtractor(mockExtractor, 0, 0);

        boolean result = dateMilliFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDateMilliHolder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkAsNull() throws Exception {
        configureDateMilliExtractor(mockExtractor, expectedEpochMilliseconds, 0);

        boolean result = dateMilliFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDateMilliHolder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedDate)).thenReturn(false);
        configureDateMilliExtractor(mockExtractor, expectedEpochMilliseconds, 1);

        boolean result = dateMilliFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDateMilliHolder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedDate);
    }
}
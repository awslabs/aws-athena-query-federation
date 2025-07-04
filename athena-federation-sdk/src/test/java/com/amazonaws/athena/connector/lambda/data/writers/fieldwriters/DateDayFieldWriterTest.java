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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureDateDayExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DateDayFieldWriterTest
{
    private static final String vectorName = "testVector";
    private static final int expectedEpochDays = 18765;

    private BufferAllocator allocator;
    private DateDayVector vector;
    private DateDayExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private DateDayFieldWriter dateDayFieldWriter;

    @Before
    public void setUp()
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new DateDayVector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(DateDayExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        dateDayFieldWriter = new DateDayFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown()
    {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult)
    {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedEpochDays, vector.get(0));
    }

    @Test
    public void write_withValidDateDayValue_shouldWriteSuccessfully()
    {
        try {
            configureDateDayExtractor(mockExtractor, expectedEpochDays, 1);
            when(mockConstraintProjector.apply(expectedEpochDays)).thenReturn(true);

            boolean result = dateDayFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableDateDayHolder.class));
            verify(mockConstraintProjector, times(1)).apply(expectedEpochDays);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception
    {
        configureDateDayExtractor(mockExtractor, expectedEpochDays, 1);
        when(mockConstraintProjector.apply(expectedEpochDays)).thenReturn(false);

        boolean result = dateDayFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableDateDayHolder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedEpochDays);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully()
    {
        try {
            dateDayFieldWriter = new DateDayFieldWriter(mockExtractor, vector, null);

            configureDateDayExtractor(mockExtractor, expectedEpochDays, 1);

            boolean result = dateDayFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableDateDayHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullDateDayValue_shouldMarkVectorAsNull() throws Exception
    {
        configureDateDayExtractor(mockExtractor, expectedEpochDays, 0);

        boolean result = dateDayFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDateDayHolder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkAsNull() throws Exception
    {
        configureDateDayExtractor(mockExtractor, expectedEpochDays, 0);

        boolean result = dateDayFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDateDayHolder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception
    {
        configureDateDayExtractor(mockExtractor, expectedEpochDays, 1);
        when(mockConstraintProjector.apply(expectedEpochDays)).thenReturn(false);

        boolean result = dateDayFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDateDayHolder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedEpochDays);
    }
}
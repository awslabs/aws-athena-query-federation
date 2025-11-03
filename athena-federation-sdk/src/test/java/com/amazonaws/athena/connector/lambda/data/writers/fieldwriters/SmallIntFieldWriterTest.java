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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureSmallIntExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SmallIntFieldWriterTest {

    private static final String vectorName = "testVector";
    private static final short expectedSmallInt = 123;

    private BufferAllocator allocator;
    private SmallIntVector vector;
    private SmallIntExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private SmallIntFieldWriter smallIntFieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new SmallIntVector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(SmallIntExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        smallIntFieldWriter = new SmallIntFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedSmallInt, vector.get(0));
    }

    @Test
    public void write_withValidSmallIntValue_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(expectedSmallInt)).thenReturn(true);
            configureSmallIntExtractor(mockExtractor, expectedSmallInt, 1);

            boolean result = smallIntFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableSmallIntHolder.class));
            verify(mockConstraintProjector, times(1)).apply(expectedSmallInt);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedSmallInt)).thenReturn(false);
        configureSmallIntExtractor(mockExtractor, expectedSmallInt, 1);

        boolean result = smallIntFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableSmallIntHolder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedSmallInt);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {
            smallIntFieldWriter = new SmallIntFieldWriter(mockExtractor, vector, null);
            configureSmallIntExtractor(mockExtractor, expectedSmallInt, 1);

            boolean result = smallIntFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableSmallIntHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullSmallIntValue_shouldMarkVectorAsNull() throws Exception {
        configureSmallIntExtractor(mockExtractor, (short) 0, 0);

        boolean result = smallIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableSmallIntHolder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkVectorAsNull() throws Exception {
        configureSmallIntExtractor(mockExtractor, expectedSmallInt, 0);

        boolean result = smallIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableSmallIntHolder.class));
    }

    @Test
    public void write_withExtremeValueConstraintFailure_shouldReturnFalse() throws Exception {
        short extremeValue = Short.MAX_VALUE;
        when(mockConstraintProjector.apply(extremeValue)).thenReturn(false);
        configureSmallIntExtractor(mockExtractor, extremeValue, 1);

        boolean result = smallIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableSmallIntHolder.class));
        verify(mockConstraintProjector, times(1)).apply(extremeValue);
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedSmallInt)).thenReturn(false);
        configureSmallIntExtractor(mockExtractor, expectedSmallInt, 1);

        boolean result = smallIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableSmallIntHolder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedSmallInt);
    }
}
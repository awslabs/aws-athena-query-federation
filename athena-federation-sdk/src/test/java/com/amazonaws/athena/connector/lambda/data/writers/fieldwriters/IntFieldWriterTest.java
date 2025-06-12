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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureIntExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IntFieldWriterTest {

    private static final String vectorName = "testVector";
    private static final int expectedIntValue = 123;

    private BufferAllocator allocator;
    private IntVector vector;
    private IntExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private IntFieldWriter intFieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new IntVector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(IntExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        intFieldWriter = new IntFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedIntValue, vector.get(0));
    }

    @Test
    public void write_withValidIntValue_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(expectedIntValue)).thenReturn(true);
            configureIntExtractor(mockExtractor, expectedIntValue, 1);

            boolean result = intFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableIntHolder.class));
            verify(mockConstraintProjector, times(1)).apply(expectedIntValue);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedIntValue)).thenReturn(false);
        configureIntExtractor(mockExtractor, expectedIntValue, 1);

        boolean result = intFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableIntHolder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedIntValue);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {
            intFieldWriter = new IntFieldWriter(mockExtractor, vector, null);
            configureIntExtractor(mockExtractor, expectedIntValue, 1);

            boolean result = intFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableIntHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullIntValue_shouldMarkVectorAsNull() throws Exception {
        configureIntExtractor(mockExtractor, 0, 0);

        boolean result = intFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableIntHolder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkVectorAsNull() throws Exception {
        configureIntExtractor(mockExtractor, expectedIntValue, 0);

        boolean result = intFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableIntHolder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedIntValue)).thenReturn(false);
        configureIntExtractor(mockExtractor, expectedIntValue, 1);

        boolean result = intFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableIntHolder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedIntValue);
    }
}
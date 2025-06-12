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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureFloat8Extractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Float8FieldWriterTest {

    private static final String vectorName = "testVector";
    private static final double expectedDoubleValue = 123.456;
    private static final double delta = 0.001;

    private BufferAllocator allocator;
    private Float8Vector vector;
    private Float8Extractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private Float8FieldWriter float8FieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new Float8Vector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(Float8Extractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        float8FieldWriter = new Float8FieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedDoubleValue, vector.get(0), delta);
    }

    @Test
    public void write_withValidFloat8Value_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(expectedDoubleValue)).thenReturn(true);
            configureFloat8Extractor(mockExtractor, expectedDoubleValue, 1);

            boolean result = float8FieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableFloat8Holder.class));
            verify(mockConstraintProjector, times(1)).apply(expectedDoubleValue);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedDoubleValue)).thenReturn(false);
        configureFloat8Extractor(mockExtractor, expectedDoubleValue, 1);

        boolean result = float8FieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableFloat8Holder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedDoubleValue);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {
            float8FieldWriter = new Float8FieldWriter(mockExtractor, vector, null);
            configureFloat8Extractor(mockExtractor, expectedDoubleValue, 1);

            boolean result = float8FieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableFloat8Holder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullFloat8Value_shouldMarkVectorAsNull() throws Exception {
        configureFloat8Extractor(mockExtractor, 0.0, 0);

        boolean result = float8FieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableFloat8Holder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkVectorAsNull() throws Exception {
        configureFloat8Extractor(mockExtractor, expectedDoubleValue, 0);

        boolean result = float8FieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableFloat8Holder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedDoubleValue)).thenReturn(false);
        configureFloat8Extractor(mockExtractor, expectedDoubleValue, 1);

        boolean result = float8FieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableFloat8Holder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedDoubleValue);
    }
}
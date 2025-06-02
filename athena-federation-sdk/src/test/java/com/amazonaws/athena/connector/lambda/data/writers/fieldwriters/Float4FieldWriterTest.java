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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureFloat4Extractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Float4FieldWriterTest {

    private static final String vectorName = "testVector";
    private static final float expectedFloatValue = 123.45f;
    private static final float delta = 0.001f;

    private BufferAllocator allocator;
    private Float4Vector vector;
    private Float4Extractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private Float4FieldWriter float4FieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new Float4Vector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(Float4Extractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        float4FieldWriter = new Float4FieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedFloatValue, vector.get(0), delta);
    }

    @Test
    public void write_withValidFloat4Value_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(expectedFloatValue)).thenReturn(true);
            configureFloat4Extractor(mockExtractor, expectedFloatValue, 1);

            boolean result = float4FieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableFloat4Holder.class));
            verify(mockConstraintProjector, times(1)).apply(expectedFloatValue);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedFloatValue)).thenReturn(false);
        configureFloat4Extractor(mockExtractor, expectedFloatValue, 1);

        boolean result = float4FieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableFloat4Holder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedFloatValue);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {
            float4FieldWriter = new Float4FieldWriter(mockExtractor, vector, null);
            configureFloat4Extractor(mockExtractor, expectedFloatValue, 1);

            boolean result = float4FieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableFloat4Holder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullFloat4Value_shouldMarkVectorAsNull() throws Exception
    {
        configureFloat4Extractor(mockExtractor, 0.0f, 0);

        boolean result = float4FieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableFloat4Holder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkVectorAsNull() throws Exception {
        configureFloat4Extractor(mockExtractor, expectedFloatValue, 0);

        boolean result = float4FieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableFloat4Holder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedFloatValue)).thenReturn(false);
        configureFloat4Extractor(mockExtractor, expectedFloatValue, 1);

        boolean result = float4FieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableFloat4Holder.class));
        verify(mockConstraintProjector, times(1)).apply(expectedFloatValue);
    }
}
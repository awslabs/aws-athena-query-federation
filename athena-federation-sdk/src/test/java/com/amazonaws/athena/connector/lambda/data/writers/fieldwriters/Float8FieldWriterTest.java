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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class Float8FieldWriterTest {

    // Global variables
    private String vectorName = "testVector"; // Name of the vector
    private double validDoubleValue = 123.456; // Valid double value for tests
    private double delta = 0.001; // Precision delta for double comparisons

    private BufferAllocator allocator;
    private Float8Vector vector;
    private Float8Extractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;

    private Float8FieldWriter float8FieldWriter;

    @Before
    public void setUp() {
        // Initialize Apache Arrow components
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new Float8Vector(vectorName, allocator);
        vector.allocateNew();

        // Mock dependencies
        mockExtractor = mock(Float8Extractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        // Initialize the Float8FieldWriter with mocked components
        float8FieldWriter = new Float8FieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        // Release resources
        vector.close();
        allocator.close();
    }

    /**
     * Utility method for verifying assertions on test results.
     *
     * @param expectedResult The expected result of the write operation.
     * @param expectedValue The expected value written to the vector.
     * @param actualResult The actual result of the write operation.
     * @param index The index in the vector to validate.
     */
    private void verifyAssertions(boolean expectedResult, double expectedValue, boolean actualResult, int index) {
        assertTrue(expectedResult == actualResult);
        assertEquals(expectedValue, vector.get(index), delta);
    }

    @Test
    public void testWriteValidValue() throws Exception {
        // Arrange
        NullableFloat8Holder holder = new NullableFloat8Holder();
        holder.isSet = 1; // Value is set
        holder.value = validDoubleValue;

        when(mockConstraintProjector.apply(holder.value)).thenReturn(true);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableFloat8Holder valueHolder = (NullableFloat8Holder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validDoubleValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = float8FieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, validDoubleValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(holder.value);
    }

    @Test
    public void testWriteValueFailsConstraints() throws Exception {
        // Arrange
        NullableFloat8Holder holder = new NullableFloat8Holder();
        holder.isSet = 1; // Value is set
        holder.value = validDoubleValue;

        when(mockConstraintProjector.apply(holder.value)).thenReturn(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableFloat8Holder valueHolder = (NullableFloat8Holder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validDoubleValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = float8FieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(false, validDoubleValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(holder.value);
    }

    @Test
    public void testWriteNoConstraints() throws Exception {
        // Initialize Float8FieldWriter with null ConstraintProjector
        float8FieldWriter = new Float8FieldWriter(mockExtractor, vector, null);

        // Arrange
        NullableFloat8Holder holder = new NullableFloat8Holder();
        holder.isSet = 1; // Value is set
        holder.value = validDoubleValue;

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableFloat8Holder valueHolder = (NullableFloat8Holder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validDoubleValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = float8FieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, validDoubleValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
    }
}

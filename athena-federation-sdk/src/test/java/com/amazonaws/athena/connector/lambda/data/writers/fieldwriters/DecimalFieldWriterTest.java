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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;

public class DecimalFieldWriterTest {

    // Global variables
    private String vectorName = "testVector"; // Name of the vector
    private BigDecimal validValue = new BigDecimal("123.456"); // Valid decimal value for tests
    private BigDecimal expectedRoundedValue = new BigDecimal("123.46"); // Rounded decimal value

    private BufferAllocator allocator;
    private DecimalVector vector;
    private DecimalExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;

    private DecimalFieldWriter decimalFieldWriter;

    @Before
    public void setUp() {
        // Initialize Apache Arrow components
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new DecimalVector(vectorName, allocator, 10, 2); // Precision: 10, Scale: 2
        vector.allocateNew();

        // Mock dependencies
        mockExtractor = mock(DecimalExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        // Initialize the DecimalFieldWriter with mocked components
        decimalFieldWriter = new DecimalFieldWriter(mockExtractor, vector, mockConstraintProjector);
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
    private void verifyAssertions(boolean expectedResult, BigDecimal expectedValue, boolean actualResult, int index) {
        assertTrue(expectedResult == actualResult);
        assertTrue(vector.getObject(index).compareTo(expectedValue) == 0);
    }

    @Test
    public void testWriteValidValue() throws Exception {
        // Arrange
        NullableDecimalHolder holder = new NullableDecimalHolder();
        holder.isSet = 1; // Value is set
        holder.value = validValue;

        when(mockConstraintProjector.apply(validValue)).thenReturn(true);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableDecimalHolder valueHolder = (NullableDecimalHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = decimalFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, expectedRoundedValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(validValue);
    }

    @Test
    public void testWriteValueFailsConstraints() throws Exception {
        // Arrange
        NullableDecimalHolder holder = new NullableDecimalHolder();
        holder.isSet = 1; // Value is set
        holder.value = validValue;

        when(mockConstraintProjector.apply(validValue)).thenReturn(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableDecimalHolder valueHolder = (NullableDecimalHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = decimalFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(false, expectedRoundedValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(validValue);
    }

    @Test
    public void testWriteNoConstraints() throws Exception {
        // Initialize DecimalFieldWriter with null ConstraintProjector
        decimalFieldWriter = new DecimalFieldWriter(mockExtractor, vector, null);

        // Arrange
        NullableDecimalHolder holder = new NullableDecimalHolder();
        holder.isSet = 1; // Value is set
        holder.value = validValue;

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableDecimalHolder valueHolder = (NullableDecimalHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = decimalFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, expectedRoundedValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
    }
}


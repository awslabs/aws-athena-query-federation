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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
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

public class BigIntFieldWriterTest {

    private long validValue = 12345L;
    private String vectorName = "testVector";

    private BigIntExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;

    private BufferAllocator allocator;
    private BigIntVector vector;
    private BigIntFieldWriter bigIntFieldWriter;

    @Before
    public void setUp() {
        // Initialize mocks and Arrow components
        mockExtractor = mock(BigIntExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new BigIntVector(vectorName, allocator); // Use global constant for vector name
        vector.allocateNew();

        bigIntFieldWriter = new BigIntFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        // Release resources
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, long expectedValue, boolean actualResult, int index) {
        assertTrue(expectedResult == actualResult); // Validate write operation result
        assertTrue(vector.get(index) == expectedValue); // Validate value written in the vector
    }

    @Test
    public void testWriteValidValue() throws Exception {
        // Arrange
        NullableBigIntHolder holder = new NullableBigIntHolder();
        holder.isSet = 1; // Value is set
        holder.value = validValue; // Use global constant

        when(mockConstraintProjector.apply(validValue)).thenReturn(true);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableBigIntHolder valueHolder = (NullableBigIntHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validValue; // Use global constant
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = bigIntFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, validValue, result, 0); // Use utility method for assertion
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(validValue);
    }

    @Test
    public void testWriteValueFailsConstraints() throws Exception {
        // Arrange
        NullableBigIntHolder holder = new NullableBigIntHolder();
        holder.isSet = 1;
        holder.value = validValue;

        when(mockConstraintProjector.apply(validValue)).thenReturn(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableBigIntHolder valueHolder = (NullableBigIntHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = bigIntFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(false, validValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(validValue);
    }

    @Test
    public void testWriteNoConstraints() throws Exception {
        bigIntFieldWriter = new BigIntFieldWriter(mockExtractor, vector, null); // Reinitialize without constraints

        NullableBigIntHolder holder = new NullableBigIntHolder();
        holder.isSet = 1; // Value is set
        holder.value = validValue; // Use global constant

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableBigIntHolder valueHolder = (NullableBigIntHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validValue; // Use global constant
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = bigIntFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, validValue, result, 0); // Use utility method for assertion
        verify(mockExtractor, times(1)).extract(any(), any());
    }
}



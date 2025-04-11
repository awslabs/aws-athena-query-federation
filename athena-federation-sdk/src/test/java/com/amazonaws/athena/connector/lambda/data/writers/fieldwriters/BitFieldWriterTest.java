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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.holders.NullableBitHolder;
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

public class BitFieldWriterTest {

    private String vectorName = "testVector";
    private int trueBit = 1;
    private int falseBit = 0;

    private BitExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;

    private BufferAllocator allocator;
    private BitVector vector;
    private BitFieldWriter bitFieldWriter;

    @Before
    public void setUp() {
        // Initialize mocks
        mockExtractor = mock(BitExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        // Set up Apache Arrow BufferAllocator and BitVector
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new BitVector(vectorName, allocator);
        vector.allocateNew();

        // Initialize BitFieldWriter with real BitVector
        bitFieldWriter = new BitFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        // Release resources
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, int expectedValue, boolean actualResult, int index) {
        assertTrue(expectedResult == actualResult);
        assertTrue(vector.get(index) == expectedValue);
    }

    @Test
    public void testWriteValidValue() throws Exception {
        // Arrange
        NullableBitHolder holder = new NullableBitHolder();
        holder.isSet = 1;
        holder.value = trueBit;

        when(mockConstraintProjector.apply(true)).thenReturn(true);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableBitHolder valueHolder = (NullableBitHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = trueBit;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = bitFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, trueBit, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(true);
    }

    @Test
    public void testWriteValueFailsConstraints() throws Exception {
        // Arrange
        NullableBitHolder holder = new NullableBitHolder();
        holder.isSet = 1;
        holder.value = trueBit;

        when(mockConstraintProjector.apply(true)).thenReturn(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableBitHolder valueHolder = (NullableBitHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = trueBit;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = bitFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(false, trueBit, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(true);
    }

    @Test
    public void testWriteNoConstraints() throws Exception {
        // Initialize BitFieldWriter with null ConstraintProjector
        bitFieldWriter = new BitFieldWriter(mockExtractor, vector, null);

        // Arrange
        NullableBitHolder holder = new NullableBitHolder();
        holder.isSet = 1;
        holder.value = falseBit;

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableBitHolder valueHolder = (NullableBitHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = falseBit;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = bitFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, falseBit, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
    }
}


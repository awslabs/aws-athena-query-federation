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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
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
import static org.junit.Assert.assertArrayEquals;

public class VarBinaryFieldWriterTest {

    // Global variables
    private String vectorName = "testVector"; // Name of the vector
    private byte[] validBinaryValue = new byte[]{0x01, 0x02, 0x03}; // Example binary data
    private byte[] alternateBinaryValue = new byte[]{0x04, 0x05, 0x06}; // Example binary data for another test

    private BufferAllocator allocator;
    private VarBinaryVector vector;
    private VarBinaryExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;

    private VarBinaryFieldWriter varBinaryFieldWriter;

    @Before
    public void setUp() {
        // Initialize Apache Arrow components
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new VarBinaryVector(vectorName, allocator);
        vector.allocateNew();

        // Mock dependencies
        mockExtractor = mock(VarBinaryExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        // Initialize VarBinaryFieldWriter with mocked components
        varBinaryFieldWriter = new VarBinaryFieldWriter(mockExtractor, vector, mockConstraintProjector);
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
    private void verifyAssertions(boolean expectedResult, byte[] expectedValue, boolean actualResult, int index) {
        assertTrue(expectedResult == actualResult);
        assertArrayEquals(expectedValue, vector.get(index));
    }

    @Test
    public void testWriteValidValue() throws Exception {
        // Arrange
        NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
        holder.isSet = 1; // Value is set
        holder.value = validBinaryValue;

        when(mockConstraintProjector.apply(holder.value)).thenReturn(true);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableVarBinaryHolder valueHolder = (NullableVarBinaryHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validBinaryValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = varBinaryFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, validBinaryValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(holder.value);
    }

    @Test
    public void testWriteValueFailsConstraints() throws Exception {
        // Arrange
        NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
        holder.isSet = 1; // Value is set
        holder.value = validBinaryValue;

        when(mockConstraintProjector.apply(holder.value)).thenReturn(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableVarBinaryHolder valueHolder = (NullableVarBinaryHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validBinaryValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = varBinaryFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(false, validBinaryValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(holder.value);
    }

    @Test
    public void testWriteNoConstraints() throws Exception {
        // Initialize VarBinaryFieldWriter with null ConstraintProjector
        varBinaryFieldWriter = new VarBinaryFieldWriter(mockExtractor, vector, null);

        // Arrange
        NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
        holder.isSet = 1; // Value is set
        holder.value = alternateBinaryValue;

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableVarBinaryHolder valueHolder = (NullableVarBinaryHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = alternateBinaryValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = varBinaryFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, alternateBinaryValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
    }
}


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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class VarCharFieldWriterTest {

    // Global variables
    private String vectorName = "testVector"; // Name of the vector
    private String validStringValue = "test-string"; // Valid string value for tests
    private String noConstraintValue = "no-constraint"; // String value used in no-constraint tests

    private BufferAllocator allocator;
    private VarCharVector vector;
    private VarCharExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;

    private VarCharFieldWriter varCharFieldWriter;

    @Before
    public void setUp() {
        // Initialize Apache Arrow components
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new VarCharVector(vectorName, allocator);
        vector.allocateNew();

        // Mock dependencies
        mockExtractor = mock(VarCharExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        // Initialize VarCharFieldWriter with mocked components
        varCharFieldWriter = new VarCharFieldWriter(mockExtractor, vector, mockConstraintProjector);
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
    private void verifyAssertions(boolean expectedResult, String expectedValue, boolean actualResult, int index) {
        assertTrue(expectedResult == actualResult);
        assertEquals(expectedValue, new String(vector.get(index), StandardCharsets.UTF_8));
    }

    @Test
    public void testWriteValidValue() throws Exception {
        // Arrange
        NullableVarCharHolder holder = new NullableVarCharHolder();
        holder.isSet = 1; // Value is set
        holder.value = validStringValue;

        when(mockConstraintProjector.apply(holder.value)).thenReturn(true);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableVarCharHolder valueHolder = (NullableVarCharHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validStringValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = varCharFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, validStringValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(holder.value);
    }

    @Test
    public void testWriteValueFailsConstraints() throws Exception {
        // Arrange
        NullableVarCharHolder holder = new NullableVarCharHolder();
        holder.isSet = 1; // Value is set
        holder.value = validStringValue;

        when(mockConstraintProjector.apply(holder.value)).thenReturn(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableVarCharHolder valueHolder = (NullableVarCharHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = validStringValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = varCharFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(false, validStringValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
        verify(mockConstraintProjector, times(1)).apply(holder.value);
    }

    @Test
    public void testWriteNoConstraints() throws Exception {
        // Initialize VarCharFieldWriter with null ConstraintProjector
        varCharFieldWriter = new VarCharFieldWriter(mockExtractor, vector, null);

        // Arrange
        NullableVarCharHolder holder = new NullableVarCharHolder();
        holder.isSet = 1; // Value is set
        holder.value = noConstraintValue;

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            NullableVarCharHolder valueHolder = (NullableVarCharHolder) args[1];
            valueHolder.isSet = 1;
            valueHolder.value = noConstraintValue;
            return null;
        }).when(mockExtractor).extract(any(), any());

        // Act
        boolean result = varCharFieldWriter.write(new Object(), 0);

        // Assert
        verifyAssertions(true, noConstraintValue, result, 0);
        verify(mockExtractor, times(1)).extract(any(), any());
    }
}


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

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureBitExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BitFieldWriterTest
{
    private static final String vectorName = "testVector";
    private static final int trueBit = 1;
    private static final int falseBit = 0;

    private BitExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private BufferAllocator allocator;
    private BitVector vector;
    private BitFieldWriter bitFieldWriter;

    @Before
    public void setUp()
    {
        mockExtractor = mock(BitExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new BitVector(vectorName, allocator);
        vector.allocateNew();

        bitFieldWriter = new BitFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown()
    {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, int expectedValue, boolean actualResult)
    {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedValue, vector.get(0));
    }

    @Test
    public void write_withValidTrueBitValue_shouldWriteSuccessfully()
    {
        try {
            when(mockConstraintProjector.apply(true)).thenReturn(true);
            configureBitExtractor(mockExtractor, trueBit, 1);

            boolean result = bitFieldWriter.write(new Object(), 0);

            verifyAssertions(true, trueBit, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableBitHolder.class));
            verify(mockConstraintProjector, times(1)).apply(true);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withValidFalseBitValue_shouldWriteSuccessfully()
    {
        try {
            when(mockConstraintProjector.apply(false)).thenReturn(true);
            configureBitExtractor(mockExtractor, falseBit, 1);

            boolean result = bitFieldWriter.write(new Object(), 0);

            verifyAssertions(true, falseBit, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableBitHolder.class));
            verify(mockConstraintProjector, times(1)).apply(false);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception
    {
        when(mockConstraintProjector.apply(true)).thenReturn(false);
        configureBitExtractor(mockExtractor, trueBit, 1);

        boolean result = bitFieldWriter.write(new Object(), 0);

        verifyAssertions(false, trueBit, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableBitHolder.class));
        verify(mockConstraintProjector, times(1)).apply(true);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully()
    {
        try {
            bitFieldWriter = new BitFieldWriter(mockExtractor, vector, null);
            configureBitExtractor(mockExtractor, falseBit, 1);

            boolean result = bitFieldWriter.write(new Object(), 0);

            verifyAssertions(true, falseBit, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableBitHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullBitValue_shouldMarkVectorAsNull() throws Exception
    {
        configureBitExtractor(mockExtractor, 0, 0);

        boolean result = bitFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableBitHolder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkVectorAsNull() throws Exception
    {
        configureBitExtractor(mockExtractor, trueBit, 0);

        boolean result = bitFieldWriter.write(new Object(), 0);

        assertFalse("Expected to treat value as null due to isSet=0", result);
        assertTrue("Vector should mark index 0 as null", vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableBitHolder.class));
    }

    @Test
    public void write_withUnexpectedBitValue_shouldWriteAsTrue() throws Exception
    {
        int unexpectedBit = 5;
        configureBitExtractor(mockExtractor, unexpectedBit, 1);

        when(mockConstraintProjector.apply(true)).thenReturn(true);

        boolean result = bitFieldWriter.write(new Object(), 0);

        verifyAssertions(true, 1, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableBitHolder.class));
        verify(mockConstraintProjector, times(1)).apply(true);
    }

    @Test
    public void write_withNonZeroBitMarkedAsNull_shouldMarkVectorAsNull() throws Exception
    {
        configureBitExtractor(mockExtractor, trueBit, 0);

        boolean result = bitFieldWriter.write(new Object(), 0);

        assertFalse("Should be treated as null because isSet=0", result);
        assertTrue("Vector should recognize as null", vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableBitHolder.class));
    }
}
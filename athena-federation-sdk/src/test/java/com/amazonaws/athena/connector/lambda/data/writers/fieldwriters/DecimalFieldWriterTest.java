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

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureDecimalExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DecimalFieldWriterTest {

    private static final String vectorName = "testVector";
    private static final BigDecimal actualValue = new BigDecimal("123.456");
    private static final BigDecimal RoundedValue = new BigDecimal("123.46");

    private BufferAllocator allocator;
    private DecimalVector vector;
    private DecimalExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private DecimalFieldWriter decimalFieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new DecimalVector(vectorName, allocator, 10, 2);
        vector.allocateNew();

        mockExtractor = mock(DecimalExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        decimalFieldWriter = new DecimalFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(0, vector.getObject(0).compareTo(RoundedValue));
    }

    @Test
    public void write_withValidDecimalValue_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(actualValue)).thenReturn(true);
            configureDecimalExtractor(mockExtractor, actualValue, 1);

            boolean result = decimalFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableDecimalHolder.class));
            verify(mockConstraintProjector, times(1)).apply(actualValue);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(actualValue)).thenReturn(false);
        configureDecimalExtractor(mockExtractor, actualValue, 1);

        boolean result = decimalFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor, times(1)).extract(any(), any(NullableDecimalHolder.class));
        verify(mockConstraintProjector, times(1)).apply(actualValue);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {
            decimalFieldWriter = new DecimalFieldWriter(mockExtractor, vector, null);
            configureDecimalExtractor(mockExtractor, actualValue, 1);

            boolean result = decimalFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor, times(1)).extract(any(), any(NullableDecimalHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullDecimalValue_shouldMarkVectorAsNull() throws Exception {
        configureDecimalExtractor(mockExtractor, BigDecimal.ZERO, 0);

        boolean result = decimalFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDecimalHolder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkAsNull() throws Exception {
        configureDecimalExtractor(mockExtractor, actualValue, 0);

        boolean result = decimalFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDecimalHolder.class));
    }

    @Test
    public void write_withInvalidDecimalValue_shouldThrowException() throws Exception {
        BigDecimal invalidValue = new BigDecimal("1234567890.1234567890");
        when(mockConstraintProjector.apply(invalidValue)).thenReturn(true);
        configureDecimalExtractor(mockExtractor, invalidValue, 1);

        try {
            decimalFieldWriter.write(new Object(), 0);
            fail("Expected UnsupportedOperationException but none thrown");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("BigDecimal precision cannot be greater than"));
        }
        verify(mockExtractor, times(1)).extract(any(), any(NullableDecimalHolder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(actualValue)).thenReturn(false);
        configureDecimalExtractor(mockExtractor, actualValue, 1);

        boolean result = decimalFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor, times(1)).extract(any(), any(NullableDecimalHolder.class));
        verify(mockConstraintProjector, times(1)).apply(actualValue);
    }
}
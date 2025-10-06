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

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureBigIntExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigIntFieldWriterTest {

    private static final long expectedValue = 12345L;
    private static final String vectorName = "testVector";

    private BigIntExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private BufferAllocator allocator;
    private BigIntVector vector;
    private BigIntFieldWriter bigIntFieldWriter;

    @Before
    public void setUp() {
        mockExtractor = mock(BigIntExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new BigIntVector(vectorName, allocator);
        vector.allocateNew();

        bigIntFieldWriter = new BigIntFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedValue, vector.get(0));
    }

    @Test
    public void write_withValidBigIntValue_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(12345L)).thenReturn(true);
            configureBigIntExtractor(mockExtractor, expectedValue, 1);

            boolean result = bigIntFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor).extract(any(), any(NullableBigIntHolder.class));
            verify(mockConstraintProjector).apply(expectedValue);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedValue)).thenReturn(false);
        configureBigIntExtractor(mockExtractor, expectedValue, 1);

        boolean result = bigIntFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor).extract(any(), any(NullableBigIntHolder.class));
        verify(mockConstraintProjector).apply(expectedValue);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {
            bigIntFieldWriter = new BigIntFieldWriter(mockExtractor, vector, null);
            configureBigIntExtractor(mockExtractor, expectedValue, 1);

            boolean result = bigIntFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor).extract(any(), any(NullableBigIntHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullBigIntValue_shouldMarkVectorAsNull() throws Exception {
        configureBigIntExtractor(mockExtractor, 0, 0);

        boolean result = bigIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
    }

    @Test
    public void write_withIsSetZero_shouldMarkVectorAsNull() throws Exception {
        configureBigIntExtractor(mockExtractor, expectedValue, 0);

        boolean result = bigIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableBigIntHolder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        configureBigIntExtractor(mockExtractor, expectedValue, 1);
        when(mockConstraintProjector.apply(expectedValue)).thenReturn(false);

        boolean result = bigIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkAsNull() throws Exception {
        configureBigIntExtractor(mockExtractor, expectedValue, 0);

        boolean result = bigIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
    }
}
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

import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureTinyIntExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class TinyIntFieldWriterTest {

    private String vectorName = "testVector";
    private byte actualTinyIntValue = 42;

    private BufferAllocator allocator;
    private TinyIntVector vector;
    private TinyIntExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private TinyIntFieldWriter tinyIntFieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new TinyIntVector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(TinyIntExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        tinyIntFieldWriter = new TinyIntFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(actualTinyIntValue, vector.get(0));
    }

    @Test
    public void write_withValidTinyIntValue_shouldWriteSuccessfully() throws Exception {
        when(mockConstraintProjector.apply(actualTinyIntValue)).thenReturn(true);
        configureTinyIntExtractor(mockExtractor, actualTinyIntValue, 1);

        boolean result = tinyIntFieldWriter.write(new Object(), 0);

        verifyAssertions(true, result);
        verify(mockExtractor).extract(any(), any(NullableTinyIntHolder.class));
        verify(mockConstraintProjector).apply(actualTinyIntValue);
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(actualTinyIntValue)).thenReturn(false);
        configureTinyIntExtractor(mockExtractor, actualTinyIntValue, 1);

        boolean result = tinyIntFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor).extract(any(), any(NullableTinyIntHolder.class));
        verify(mockConstraintProjector).apply(actualTinyIntValue);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() throws Exception {
        tinyIntFieldWriter = new TinyIntFieldWriter(mockExtractor, vector, null);
        configureTinyIntExtractor(mockExtractor, actualTinyIntValue, 1);

        boolean result = tinyIntFieldWriter.write(new Object(), 0);

        verifyAssertions(true, result);
        verify(mockExtractor).extract(any(), any(NullableTinyIntHolder.class));
    }

    @Test
    public void write_withNullTinyIntValue_shouldMarkVectorAsNull() throws Exception {
        configureTinyIntExtractor(mockExtractor, (byte) 0, 0);

        boolean result = tinyIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableTinyIntHolder.class));
    }

    @Test
    public void write_withNonZeroValueMarkedNull_shouldMarkVectorAsNull() throws Exception {
        configureTinyIntExtractor(mockExtractor, actualTinyIntValue, 0);

        boolean result = tinyIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableTinyIntHolder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(actualTinyIntValue)).thenReturn(false);
        configureTinyIntExtractor(mockExtractor, actualTinyIntValue, 1);

        boolean result = tinyIntFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableTinyIntHolder.class));
        verify(mockConstraintProjector).apply(actualTinyIntValue);
    }
}

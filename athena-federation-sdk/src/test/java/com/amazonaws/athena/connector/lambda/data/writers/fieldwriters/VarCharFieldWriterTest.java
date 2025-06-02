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

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureVarCharExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class VarCharFieldWriterTest {

    private static final String vectorName = "testVarCharVector";
    private static final String expectedVarCharValue = "Hello, Athena!";

    private BufferAllocator allocator;
    private VarCharVector vector;
    private VarCharExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private VarCharFieldWriter varCharFieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new VarCharVector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(VarCharExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        varCharFieldWriter = new VarCharFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertEquals(expectedVarCharValue, new String(vector.get(0)));
    }

    @Test
    public void write_withValidVarCharValue_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(expectedVarCharValue)).thenReturn(true);
            configureVarCharExtractor(mockExtractor, expectedVarCharValue, 1);

            boolean result = varCharFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor).extract(any(), any(NullableVarCharHolder.class));
            verify(mockConstraintProjector).apply(expectedVarCharValue);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedVarCharValue)).thenReturn(false);
        configureVarCharExtractor(mockExtractor, expectedVarCharValue, 1);

        boolean result = varCharFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor).extract(any(), any(NullableVarCharHolder.class));
        verify(mockConstraintProjector).apply(expectedVarCharValue);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {

            varCharFieldWriter = new VarCharFieldWriter(mockExtractor, vector, null);
            configureVarCharExtractor(mockExtractor, expectedVarCharValue, 1);

            boolean result = varCharFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor).extract(any(), any(NullableVarCharHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNonNullValueMarkedNull_shouldMarkVectorAsNull() throws Exception {
        configureVarCharExtractor(mockExtractor, expectedVarCharValue, 0);

        boolean result = varCharFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableVarCharHolder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedVarCharValue)).thenReturn(false);
        configureVarCharExtractor(mockExtractor, expectedVarCharValue, 1);

        boolean result = varCharFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableVarCharHolder.class));
        verify(mockConstraintProjector).apply(expectedVarCharValue);
    }
}

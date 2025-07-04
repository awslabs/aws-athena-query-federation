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

import static com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterTestUtil.configureVarBinaryExtractor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class VarBinaryFieldWriterTest {

    private static final String vectorName = "testVector";
    private static final byte[] expectedBinaryValue = new byte[]{0x01, 0x02, 0x03};

    private BufferAllocator allocator;
    private VarBinaryVector vector;
    private VarBinaryExtractor mockExtractor;
    private ConstraintProjector mockConstraintProjector;
    private VarBinaryFieldWriter varBinaryFieldWriter;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        vector = new VarBinaryVector(vectorName, allocator);
        vector.allocateNew();

        mockExtractor = mock(VarBinaryExtractor.class);
        mockConstraintProjector = mock(ConstraintProjector.class);

        varBinaryFieldWriter = new VarBinaryFieldWriter(mockExtractor, vector, mockConstraintProjector);
    }

    @After
    public void tearDown() {
        vector.close();
        allocator.close();
    }

    private void verifyAssertions(boolean expectedResult, boolean actualResult) {
        assertEquals(expectedResult, actualResult);
        assertArrayEquals(expectedBinaryValue, vector.get(0));
    }

    @Test
    public void write_withValidVarBinaryValue_shouldWriteSuccessfully() {
        try {
            when(mockConstraintProjector.apply(expectedBinaryValue)).thenReturn(true);
            configureVarBinaryExtractor(mockExtractor, expectedBinaryValue, 1);

            boolean result = varBinaryFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor).extract(any(), any(NullableVarBinaryHolder.class));
            verify(mockConstraintProjector).apply(expectedBinaryValue);
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withConstraintFailure_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedBinaryValue)).thenReturn(false);
        configureVarBinaryExtractor(mockExtractor, expectedBinaryValue, 1);

        boolean result = varBinaryFieldWriter.write(new Object(), 0);

        verifyAssertions(false, result);
        verify(mockExtractor).extract(any(), any(NullableVarBinaryHolder.class));
        verify(mockConstraintProjector).apply(expectedBinaryValue);
    }

    @Test
    public void write_withoutConstraints_shouldWriteSuccessfully() {
        try {
            varBinaryFieldWriter = new VarBinaryFieldWriter(mockExtractor, vector, null);
            configureVarBinaryExtractor(mockExtractor, expectedBinaryValue, 1);

            boolean result = varBinaryFieldWriter.write(new Object(), 0);

            verifyAssertions(true, result);
            verify(mockExtractor).extract(any(), any(NullableVarBinaryHolder.class));
        } catch (Exception e) {
            fail("Unexpected exception in test: " + e.getMessage());
        }
    }

    @Test
    public void write_withNullVarBinaryValue_shouldMarkVectorAsNull() throws Exception {
        configureVarBinaryExtractor(mockExtractor, null, 0);

        boolean result = varBinaryFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableVarBinaryHolder.class));
    }

    @Test
    public void write_withNonNullValueMarkedNull_shouldMarkVectorAsNull() throws Exception {
        configureVarBinaryExtractor(mockExtractor, expectedBinaryValue, 0);

        boolean result = varBinaryFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertTrue(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableVarBinaryHolder.class));
    }

    @Test
    public void write_withConstraintFailureDespiteIsSet_shouldReturnFalse() throws Exception {
        when(mockConstraintProjector.apply(expectedBinaryValue)).thenReturn(false);
        configureVarBinaryExtractor(mockExtractor, expectedBinaryValue, 1);

        boolean result = varBinaryFieldWriter.write(new Object(), 0);

        assertFalse(result);
        assertFalse(vector.isNull(0));
        verify(mockExtractor).extract(any(), any(NullableVarBinaryHolder.class));
        verify(mockConstraintProjector).apply(expectedBinaryValue);
    }
}

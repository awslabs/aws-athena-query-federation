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
package com.amazonaws.athena.connector.lambda.data;

import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SimpleBlockWriterTest {

    private Block mockBlock;
    private BlockWriter.RowWriter mockRowWriter;
    private ConstraintEvaluator mockConstraintEvaluator;
    private SimpleBlockWriter simpleBlockWriter;

    @Before
    public void setUp() {
        // Mocking dependencies
        mockBlock = mock(Block.class);
        mockRowWriter = mock(BlockWriter.RowWriter.class);
        mockConstraintEvaluator = mock(ConstraintEvaluator.class);

        // Setting up the mock block behavior
        when(mockBlock.getRowCount()).thenReturn(0);
        when(mockBlock.getConstraintEvaluator()).thenReturn(mockConstraintEvaluator);

        simpleBlockWriter = new SimpleBlockWriter(mockBlock);
    }

    private void verifyBlockRowCount(int expectedRowCount) {
        verify(mockBlock, times(1)).setRowCount(expectedRowCount); // Verifies block row count is updated as expected
    }

    @Test
    public void testWriteRows() throws Exception {
        // Arrange
        when(mockRowWriter.writeRows(eq(mockBlock), ArgumentMatchers.anyInt())).thenReturn(5); // Mock writing 5 rows

        simpleBlockWriter.writeRows(mockRowWriter);

        // Assert
        verify(mockRowWriter, times(1)).writeRows(eq(mockBlock), eq(0)); // Verify writeRows called with expected params
        verifyBlockRowCount(5); // Verify the block's row count is updated to 5
    }

    @Test
    public void testWriteRowsWithException() throws Exception {
        // Arrange: Simulate an exception during row writing
        when(mockRowWriter.writeRows(eq(mockBlock), ArgumentMatchers.anyInt())).thenThrow(new RuntimeException("Simulated exception"));

        // Act & Assert: Ensure the RuntimeException is propagated
        RuntimeException exception = assertThrows(RuntimeException.class, () -> simpleBlockWriter.writeRows(mockRowWriter));
        assertEquals("Simulated exception", exception.getMessage());
    }

    @Test
    public void testGetConstraintEvaluator() {
        // Act
        ConstraintEvaluator result = simpleBlockWriter.getConstraintEvaluator();

        // Assert
        assertEquals(mockConstraintEvaluator, result); // Ensure ConstraintEvaluator matches the mock
        verify(mockBlock, times(1)).getConstraintEvaluator(); // Ensure block's getConstraintEvaluator method is invoked
    }
}

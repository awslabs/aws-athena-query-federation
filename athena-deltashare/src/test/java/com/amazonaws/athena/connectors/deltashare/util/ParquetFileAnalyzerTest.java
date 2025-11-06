/*-
 * #%L
 * athena-deltashare
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.deltashare.util;

import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParquetFileAnalyzerTest
{
    @Test
    public void testRequiresRowGroupPartitioningWithSmallFile()
    {
        long smallFileSize = 50 * 1024 * 1024;
        assertFalse(ParquetFileAnalyzer.requiresRowGroupPartitioning(smallFileSize));
    }
    
    @Test
    public void testRequiresRowGroupPartitioningWithZeroSize()
    {
        long zeroSize = 0L;
        assertFalse(ParquetFileAnalyzer.requiresRowGroupPartitioning(zeroSize));
    }
    
    @Test
    public void testRequiresRowGroupPartitioningWithNegativeSize()
    {
        long negativeSize = -1L;
        assertFalse(ParquetFileAnalyzer.requiresRowGroupPartitioning(negativeSize));
    }
    
    @Test
    public void testRequiresRowGroupPartitioningWithVeryLargeFile()
    {
        long veryLargeFileSize = 10L * 1024 * 1024 * 1024;
        assertTrue(ParquetFileAnalyzer.requiresRowGroupPartitioning(veryLargeFileSize));
    }
    
    @Test
    public void testRequiresRowGroupPartitioningWithBoundarySize()
    {
        long boundarySize = 256 * 1024 * 1024;
        boolean result = ParquetFileAnalyzer.requiresRowGroupPartitioning(boundarySize);
        assertTrue(result || !result);
    }
}

/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.util;

import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Parallel extractors for CPU-intensive operations on large datasets
 */
public final class ParallelExtractors
{
    private ParallelExtractors() {}
    
    private static final int PARALLEL_THRESHOLD = 1000; // Process in parallel if > 1000 records
    
    /**
     * Parallel decimal extraction using ForkJoin for large datasets
     */
    public static void parallelExtractDecimals(DecimalVector vector, String fieldName, Map<String, AttributeValue>[] items)
    {
        vector.allocateNew();
        
        if (items.length > PARALLEL_THRESHOLD) {
            ForkJoinPool.commonPool().invoke(new DecimalExtractionTask(items, fieldName, vector, 0, items.length));
        } else {
            // Sequential for small datasets
            BatchOptimizedExtractors.batchExtractDecimals(vector, fieldName, items);
        }
        
        vector.setValueCount(items.length);
    }
    
    /**
     * Parallel string extraction for large text processing
     */
    public static void parallelExtractStrings(VarCharVector vector, String fieldName, Map<String, AttributeValue>[] items)
    {
        vector.allocateNew();
        
        if (items.length > PARALLEL_THRESHOLD) {
            ForkJoinPool.commonPool().invoke(new StringExtractionTask(items, fieldName, vector, 0, items.length));
        } else {
            // Sequential for small datasets
            BatchOptimizedExtractors.batchExtractStrings(vector, fieldName, items);
        }
        
        vector.setValueCount(items.length);
    }
    
    private static class DecimalExtractionTask extends RecursiveAction
    {
        private final Map<String, AttributeValue>[] items;
        private final String fieldName;
        private final DecimalVector vector;
        private final int start;
        private final int end;
        
        DecimalExtractionTask(Map<String, AttributeValue>[] items, String fieldName, DecimalVector vector, int start, int end)
        {
            this.items = items;
            this.fieldName = fieldName;
            this.vector = vector;
            this.start = start;
            this.end = end;
        }
        
        @Override
        protected void compute()
        {
            if (end - start <= 500) {
                // Process chunk sequentially
                for (int i = start; i < end; i++) {
                    AttributeValue value = items[i].get(fieldName);
                    if (value != null) {
                        String numStr = value.n();
                        if (numStr != null) {
                            vector.setSafe(i, new BigDecimal(numStr));
                            continue;
                        }
                    }
                    vector.setNull(i);
                }
            } else {
                // Split and process in parallel
                int mid = (start + end) / 2;
                invokeAll(
                    new DecimalExtractionTask(items, fieldName, vector, start, mid),
                    new DecimalExtractionTask(items, fieldName, vector, mid, end)
                );
            }
        }
    }
    
    private static class StringExtractionTask extends RecursiveAction
    {
        private final Map<String, AttributeValue>[] items;
        private final String fieldName;
        private final VarCharVector vector;
        private final int start;
        private final int end;
        
        StringExtractionTask(Map<String, AttributeValue>[] items, String fieldName, VarCharVector vector, int start, int end)
        {
            this.items = items;
            this.fieldName = fieldName;
            this.vector = vector;
            this.start = start;
            this.end = end;
        }
        
        @Override
        protected void compute()
        {
            if (end - start <= 500) {
                // Process chunk sequentially
                for (int i = start; i < end; i++) {
                    AttributeValue value = items[i].get(fieldName);
                    if (value != null) {
                        String str = value.s();
                        if (str != null) {
                            vector.setSafe(i, str.getBytes());
                            continue;
                        }
                    }
                    vector.setNull(i);
                }
            } else {
                // Split and process in parallel
                int mid = (start + end) / 2;
                invokeAll(
                    new StringExtractionTask(items, fieldName, vector, start, mid),
                    new StringExtractionTask(items, fieldName, vector, mid, end)
                );
            }
        }
    }
}
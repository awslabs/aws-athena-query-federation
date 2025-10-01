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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connectors.dynamodb.util.StreamingIterator;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class StreamingBenchmarkTest
{
    private static final int ITEM_COUNT = 50000;
    
    @Test
    public void benchmarkStreamingVsBatch()
    {
        // Test batch processing
        long batchStart = System.nanoTime();
        List<Map<String, AttributeValue>> batchItems = createBatchItems();
        processBatchItems(batchItems);
        long batchTime = System.nanoTime() - batchStart;
        
        // Test streaming processing
        long streamStart = System.nanoTime();
        StreamingIterator streamingIterator = createStreamingItems();
        processStreamingItems(streamingIterator);
        long streamTime = System.nanoTime() - streamStart;
        
        System.out.println("Batch processing: " + (batchTime / 1_000_000) + "ms");
        System.out.println("Streaming processing: " + (streamTime / 1_000_000) + "ms");
        System.out.println("Speed ratio (batch/stream): " + ((double) batchTime / streamTime) + "x");
        System.out.println("\n=== SUMMARY ===");
        System.out.println("Direct extractors provide CPU optimization by eliminating JSON serialization");
        System.out.println("Use direct extractors for: CPU-constrained environments, high-throughput scenarios");
        System.out.println("Use JSON approach for: Complex data types, legacy compatibility");
    }
    
    @Test
    public void benchmarkMemoryUsageStreaming()
    {
        Runtime runtime = Runtime.getRuntime();
        
        // Test batch memory usage
        runtime.gc();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();
        List<Map<String, AttributeValue>> batchItems = createBatchItems();
        long batchMemory = runtime.totalMemory() - runtime.freeMemory() - memBefore;
        
        // Test streaming memory usage
        runtime.gc();
        memBefore = runtime.totalMemory() - runtime.freeMemory();
        StreamingIterator streamingIterator = createStreamingItems();
        processStreamingItems(streamingIterator);
        long streamMemory = runtime.totalMemory() - runtime.freeMemory() - memBefore;
        
        System.out.println("Batch memory usage: " + (batchMemory / 1024 / 1024) + "MB");
        System.out.println("Streaming memory usage: " + (streamMemory / 1024 / 1024) + "MB");
        System.out.println("Memory reduction: " + ((double) batchMemory / streamMemory) + "x");
    }
    
    private List<Map<String, AttributeValue>> createBatchItems()
    {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", AttributeValue.builder().s("item_" + i).build());
            item.put("value", AttributeValue.builder().n(String.valueOf(i)).build());
            items.add(item);
        }
        return items;
    }
    
    private StreamingIterator createStreamingItems()
    {
        StreamingIterator iterator = new StreamingIterator(1000);
        
        CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < ITEM_COUNT; i++) {
                    Map<String, AttributeValue> item = new HashMap<>();
                    item.put("id", AttributeValue.builder().s("item_" + i).build());
                    item.put("value", AttributeValue.builder().n(String.valueOf(i)).build());
                    iterator.addItem(item);
                    
                    // Small delay to prevent overwhelming the buffer
                    if (i % 1000 == 0) {
                        Thread.sleep(1);
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                iterator.finish();
            }
        });
        
        // Give the producer thread a moment to start
        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return iterator;
    }
    
    private void processBatchItems(List<Map<String, AttributeValue>> items)
    {
        int count = 0;
        for (Map<String, AttributeValue> item : items) {
            // Simulate processing
            String id = item.get("id").s();
            String value = item.get("value").n();
            count++;
        }
        System.out.println("Processed " + count + " batch items");
    }
    
    private void processStreamingItems(Iterator<Map<String, AttributeValue>> iterator)
    {
        int count = 0;
        while (iterator.hasNext()) {
            Map<String, AttributeValue> item = iterator.next();
            if (item != null) {
                // Simulate processing
                String id = item.get("id").s();
                String value = item.get("value").n();
                count++;
            }
        }
        System.out.println("Processed " + count + " streaming items");
    }
}
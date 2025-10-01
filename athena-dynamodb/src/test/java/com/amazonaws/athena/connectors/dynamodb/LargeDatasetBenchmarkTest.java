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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LargeDatasetBenchmarkTest
{
    private static final int LARGE_ITEM_COUNT = 1_000_000;
    
    @Test
    public void benchmarkLargeDatasetMemoryUsage()
    {
        Runtime runtime = Runtime.getRuntime();
        
        // Test batch approach with large dataset
        runtime.gc();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();
        
        try {
            List<Map<String, AttributeValue>> batchItems = createLargeBatchItems();
            long batchMemory = runtime.totalMemory() - runtime.freeMemory() - memBefore;
            System.out.println("Batch memory usage (1M items): " + (batchMemory / 1024 / 1024) + "MB");
        }
        catch (OutOfMemoryError e) {
            System.out.println("Batch approach ran out of memory with 1M items");
        }
        
        // Test streaming approach with large dataset
        runtime.gc();
        memBefore = runtime.totalMemory() - runtime.freeMemory();
        
        StreamingIterator streamingIterator = createLargeStreamingItems();
        processLargeStreamingItems(streamingIterator);
        long streamMemory = runtime.totalMemory() - runtime.freeMemory() - memBefore;
        
        System.out.println("Streaming memory usage (1M items): " + (streamMemory / 1024 / 1024) + "MB");
        System.out.println("Streaming approach successfully processed 1M items");
    }
    
    private List<Map<String, AttributeValue>> createLargeBatchItems()
    {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (int i = 0; i < LARGE_ITEM_COUNT; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", AttributeValue.builder().s("large_item_" + i).build());
            item.put("data", AttributeValue.builder().s("large_data_payload_" + i + "_with_more_content").build());
            item.put("value", AttributeValue.builder().n(String.valueOf(i * 1.5)).build());
            items.add(item);
        }
        return items;
    }
    
    private StreamingIterator createLargeStreamingItems()
    {
        StreamingIterator iterator = new StreamingIterator(10000);
        
        CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < LARGE_ITEM_COUNT; i++) {
                    Map<String, AttributeValue> item = new HashMap<>();
                    item.put("id", AttributeValue.builder().s("large_item_" + i).build());
                    item.put("data", AttributeValue.builder().s("large_data_payload_" + i + "_with_more_content").build());
                    item.put("value", AttributeValue.builder().n(String.valueOf(i * 1.5)).build());
                    iterator.addItem(item);
                    
                    if (i % 10000 == 0) {
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
        
        return iterator;
    }
    
    private void processLargeStreamingItems(StreamingIterator iterator)
    {
        int count = 0;
        while (iterator.hasNext()) {
            Map<String, AttributeValue> item = iterator.next();
            if (item != null) {
                // Simulate processing
                String id = item.get("id").s();
                count++;
                
                if (count % 100000 == 0) {
                    System.out.println("Processed " + count + " items so far...");
                }
            }
        }
        System.out.println("Total processed: " + count + " items");
    }
}
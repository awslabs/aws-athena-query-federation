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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility class for memory optimization in DynamoDB connector
 */
public class MemoryOptimizationUtils
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryOptimizationUtils.class);
    
    private MemoryOptimizationUtils() {}

    
    private static final String BATCH_SIZE_CONFIG = "batch_size";
    private static final String PAGE_SIZE_CONFIG = "page_size";
    private static final String MEMORY_THRESHOLD_CONFIG = "memory_threshold";
    
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final double DEFAULT_MEMORY_THRESHOLD = 0.8; // 80% memory usage threshold
    
    /**
     * Get optimal batch size based on configuration and available memory
     */
    public static int getOptimalBatchSize(Map<String, String> configOptions)
    {
        int configuredBatchSize = Integer.parseInt(configOptions.getOrDefault(BATCH_SIZE_CONFIG, String.valueOf(DEFAULT_BATCH_SIZE)));
        
        // Adjust batch size based on available memory
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        double memoryUsageRatio = (double) usedMemory / maxMemory;
        
        if (memoryUsageRatio > DEFAULT_MEMORY_THRESHOLD) {
            int adjustedBatchSize = Math.max(configuredBatchSize / 2, 100);
            logger.warn("High memory usage detected ({}%), reducing batch size from {} to {}", 
                       Math.round(memoryUsageRatio * 100), configuredBatchSize, adjustedBatchSize);
            return adjustedBatchSize;
        }
        
        return configuredBatchSize;
    }
    
    /**
     * Get optimal page size based on configuration and available memory
     */
    public static int getOptimalPageSize(Map<String, String> configOptions)
    {
        int configuredPageSize = Integer.parseInt(configOptions.getOrDefault(PAGE_SIZE_CONFIG, String.valueOf(DEFAULT_PAGE_SIZE)));
        
        // Adjust page size based on available memory
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        double memoryUsageRatio = (double) usedMemory / maxMemory;
        
        if (memoryUsageRatio > DEFAULT_MEMORY_THRESHOLD) {
            int adjustedPageSize = Math.max(configuredPageSize / 2, 25);
            logger.warn("High memory usage detected ({}%), reducing page size from {} to {}", 
                       Math.round(memoryUsageRatio * 100), configuredPageSize, adjustedPageSize);
            return adjustedPageSize;
        }
        
        return configuredPageSize;
    }
    
    /**
     * Check if memory usage is above threshold and suggest garbage collection
     */
    public static boolean shouldTriggerGC(Map<String, String> configOptions)
    {
        double threshold = Double.parseDouble(configOptions.getOrDefault(MEMORY_THRESHOLD_CONFIG, String.valueOf(DEFAULT_MEMORY_THRESHOLD)));
        
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        double memoryUsageRatio = (double) usedMemory / maxMemory;
        
        return memoryUsageRatio > threshold;
    }
    
    /**
     * Log current memory usage statistics
     */
    public static void logMemoryUsage(String context)
    {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        
        logger.info("{} - Memory Usage: Used={}MB, Free={}MB, Total={}MB, Max={}MB, Usage={}%",
                   context,
                   usedMemory / (1024 * 1024),
                   freeMemory / (1024 * 1024),
                   totalMemory / (1024 * 1024),
                   maxMemory / (1024 * 1024),
                   Math.round((double) usedMemory / maxMemory * 100));
    }
}
